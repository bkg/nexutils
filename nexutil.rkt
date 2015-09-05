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

(define (system/display cmd)
  (if (dry-run?)
    (displayln cmd)
    (unless (system cmd)
      (exit 1))))

(define (basename path)
  (if (url? path)
    ((compose1 path/param-path last url-path) path)
    (last (explode-path path))))

(define (raster->geotiff src [dest (path-replace-suffix src ".tif")])
  (system/display
    (~a "gdal_translate"
        "-of GTiff -co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6"
        src dest
        #:separator " "))
  dest)

(define (monthly->annual src [dest (path-replace-suffix src "_ann.nc")])
  (system/display (format "ncra -OD 1 -L6 -d time,,,12,12 --mro ~a ~a" src dest))
  dest)

(define (concat-netcdf src-list
                       [dest (path-replace-suffix (last src-list) "_all.nc")])
  (system/display (format "ncrcat -L6 ~a ~a" (string-join src-list " ") dest))
  dest)

(define (monthly->annual in out)
  (system/display (format "ncra -OD 1 -d time,,,12,12 --mro ~a ~a" in out)))

(define (find-input-files datadir)
  (find-files
    (λ (path) (equal? (filename-extension path) #"nc"))
    datadir))

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

(define (run-task item)
  (let* ([netcdf-path (path-replace-suffix item "_ann.nc")]
         [geotiff-path (path-replace-suffix netcdf-path ".tif")])
      (begin 
        (monthly->annual item netcdf-path)
        (raster->geotiff netcdf-path geotiff-path))))

(define (make-worker-thread id channel)
  (define (loop)
    (let ([item (async-channel-get channel)])
      (case item
        [(DONE) (printft "Thread ~a done" id)]
        [else
          (printft "Thread ~a processing item: ~a" id item)
          (run-task item)
          (loop)])))
  (thread loop))

(define (run-workers tasks nworkers)
  (let* ([work-channel (make-async-channel nworkers)]
         [workers (for/list ([id (in-range nworkers)])
                    (make-worker-thread id work-channel))])
    (for ([task (in-list (append tasks (make-list nworkers 'DONE)))])
      (async-channel-put work-channel task))
    (for-each thread-wait workers)))

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
                   #|(url->file data-url datadir)))|#
                   (let ([out-path (build-path datadir (basename data-url))])
                     (url->file data-url out-path)
                     out-path)))
               nthreads))
