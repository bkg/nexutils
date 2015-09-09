#!/usr/bin/env racket
#lang racket/base

(require racket/file
         racket/format
         racket/function
         (only-in racket/future processor-count)
         racket/list
         racket/path
         racket/port
         racket/string
         racket/system
         racket/async-channel
         net/url
         xml
         xml/path)

(define dry-run? (make-parameter #f))
(define verbose? (make-parameter #f))

(define (sh cmd)
  (printft cmd)
  (unless (dry-run?)
    (unless (system cmd)
      (exit 1))))

(define (system/out . cmd+args)
  (and (sh (apply ~a cmd+args #:separator " "))
       (last cmd+args)))

(define (basename path)
  (if (url? path)
    ((compose1 path/param-path last url-path) path)
    (file-name-from-path path)))

(define (raster->geotiff src [dest (path-replace-suffix src ".tif")])
  (system/out
    "gdal_translate"
    "-of GTiff -co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6"
    src dest))

(define (monthly->annual src [dest (path-replace-suffix src "_ann.nc")])
  (system/out "ncra -OD 1 -L6 -d time,,,12,12 --mro" src dest))

(define (concat-netcdf src-list
                       [dest (path-replace-suffix (last src-list) "_all.nc")])
  (system/out "ncrcat -L6" (string-join src-list " ") dest))

(define (find-input-files datadir)
  (find-files
    (λ (path) (regexp-match? #px"\\d{6}-\\d{6}\\.nc$" path))
    datadir))

(define (trim-dates str) (string-trim str #px"\\d{6}-.*" #:left? #f))

(define (group-by extract-key lst [same? equal?])
  ; Create nested list of path strings grouped by comparison.
  (foldr (λ (s acc)
           (cond
             [(null? acc) (list (list s))]
             [(apply same? (map extract-key (list s (caar acc))))
              (cons (cons s (car acc))
                    (cdr acc))]
             [else (cons (list s) acc)]))
         '()
         (sort lst string<?)))

(define (url->output url [out (current-output-port)])
  (call/input-url
    url
    get-pure-port
    (curryr copy-port out)))

;; Fetch file url, returns void
;; Arguments:
;; url - URL
;; path - filename string or directory for writing to
(define (url->file url path)
  (with-output-to-file
    (if (directory-exists? path)
      (build-path path (basename url))
      path)
    (thunk (url->output url))))

(define (fetch url)
  (call/input-url url get-pure-port port->string))

(define (list-bucket-urls prefix)
  (let* ([bucket-url (string->url "https://nasanex.s3.amazonaws.com/")]
         [request (struct-copy url bucket-url [query `((prefix . ,prefix))])]
         [s3-keys (se-path*/list '(Key) (string->xexpr (fetch request)))])
    (for/list ([key (in-list s3-keys)])
      (combine-url/relative bucket-url key))))

(define (make-worker id work-channel result-channel)
  (define (loop)
    (let ([task (async-channel-get work-channel)])
      (cond
        [(procedure? task)
         (printft "Thread ~a processing item: ~a" id task)
         (async-channel-put result-channel (task))
         (loop)]
        [else (printft "Thread ~a done" id)])))
  (thread loop))

(define (pool-map proc tasks #:workers [n 3])
  (let* ([work-channel (make-async-channel n)]
         [result-channel (make-async-channel)]
         [workers (for/list ([id (in-range n)])
                    (make-worker id work-channel result-channel))])
    (for ([task (in-list tasks)])
      (async-channel-put work-channel (lambda () (proc task))))
    (for ([worker (in-list workers)])
      (async-channel-put work-channel 'stop))
    (for/list ([task-num (in-range (length tasks))])
      (async-channel-get result-channel))))

(define (run-workers tasks nworkers)
    (define (group-paths paths) (group-by trim-dates (map path->string paths)))
    (pool-map concat-netcdf
              (group-paths (pool-map monthly->annual tasks #:workers nworkers))
              #:workers 2))

(define (put-gdal-env path)
  (let ([dirs (hash 'PATH "bin"
                    'LD_LIBRARY_PATH "lib"
                    'GDAL_DATA "share/gdal")])
    (for ([(key val) (in-hash dirs)])
      (let ([v (path->string (build-path path val))])
        (putenv (symbol->string key)
                (case key
                  [(PATH) (string-append v ":" (getenv "PATH"))]
                  [else v]))))))

(define print-thread
  (thread (λ ()
            (let loop ()
              (displayln (thread-receive))
              (loop)))))

(define (printft . items)
  (when (verbose?) (thread-send print-thread (apply format items))))

(module+ main
  (require racket/cmdline)
  (define nthreads (min (/ (processor-count) 2) 3))
  (define datadir (if (directory-exists? "/tmp")
                    "/tmp"
                    (find-system-path 'temp-dir)))
  (define skip-download? #f)
  (define prefix
    (command-line
      #:once-each
      [("-d" "--datadir") dir "Data directory"
       (set! datadir (expand-user-path dir))]
      [("-g" "--gdalbase") gdalbase "GDAL base path"
       (put-gdal-env (expand-user-path gdalbase))]
      [("-n" "--dry-run") "Dry run" (dry-run? #t)]
      [("-j" "--jobs") thread-count "Threads" (set! nthreads thread-count)]
      [("-s" "--skip-download") "Skip downloading" (set! skip-download? #t)]
      [("-v" "--verbose") "Verbose mode" (verbose? #t)]
      #:args
      (prefix) prefix))
  (run-workers (if skip-download?
                 (find-input-files datadir)
                 (for/list ([data-url (in-list (list-bucket-urls prefix))])
                   (let ([out-path (build-path datadir (basename data-url))])
                     (url->file data-url out-path)
                     out-path)))
               nthreads))
