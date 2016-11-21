#lang racket/base

(require racket/class
         racket/draw
         racket/flonum
         racket/list
         racket/match
         racket/math
         racket/sequence
         racket/vector)

(provide (all-defined-out))

;; Value of masked/transparent pixels.
(define mask-val 255)

;; Returns the color at a distance between two colors.
(define (interpolate-color c1 c2 t)
  (for/list ([ch1 (in-list c1)]
             [ch2 (in-list c2)])
    (floor (+ ch1 (* t (- ch2 ch1))))))

;; Returns a list of two interpolated colors using n steps.
(define (linear-gradient start finish [n 10])
  (for/list ([i (in-range n)])
    (interpolate-color start finish (/ i n))))

;; Returns a multicolor linear gradient as a list of rgb values.
(define (multilinear-gradient colors [n (* (length colors) 10)])
  (let ([steps (/ n (length colors))])
    (cons (car colors)
          (append-map cdr
                      (for/list ([c1 (in-list colors)]
                                 [c2 (in-list (cdr colors))])
                        (linear-gradient c1 c2 steps))))))

;; A color table created by Bill Haxby, Lamont-Doherty Earth Observatory.
(define colors-haxby
  (list->vector
    (multilinear-gradient
      '((10 0 121)
        (40 0 150)
        (20 5 175)
        (0 10 200)
        (0 25 212)
        (0 40 224)
        (26 102 240)
        (13 129 248)
        (25 175 255)
        (50 190 255)
        (68 202 255)
        (97 225 240)
        (106 235 225)
        (124 235 200)
        (138 236 174)
        (172 245 168)
        (205 255 162)
        (223 245 141)
        (240 236 121)
        (247 215 104)
        (255 189 87)
        (255 160 69)
        (244 117 75)
        (238 80 78)
        (255 90 90)
        (255 124 124)
        (255 158 158)
        (245 179 174)
        (255 196 196)
        (255 215 215)
        (255 235 235)
        (255 254 253))
      255)))

;; Returns a list of percentiles for a sequence.
(define (quantiles seq plst)
  (let* ([vsort (list->vector (sort (sequence->list seq) <))]
         [vlen (vector-length vsort)])
    (for/list ([p (in-list plst)])
      (vector-ref vsort (exact-ceiling (sub1 (* p vlen)))))))

(define (vector-extrema vect)
  (let ([v (vector-ref vect 0)])
    (for/fold ([extrema (cons v v)])
              ([val (in-vector vect 1 #f)])
      (cons (min val (car extrema))
            (max val (cdr extrema))))))

(define (vector-clip vect amin amax)
  (for/vector #:length (vector-length vect) ([v (in-vector vect)])
    (cond [(< v amin) amin]
          [(> v amax) amax]
          [else v])))

(define (vector-stretch vect)
  (let ([q (quantiles (in-vector vect 0 #f 8) '(.02 .98))])
    (vector-scale (apply vector-clip vect q) 0 254)))

;; Returns a vector scaled to a new min/max.
(define (vector-scale vect new-min new-max)
  (match-let ([(cons vmin vmax) (vector-extrema vect)])
    (let ([factor (exact->inexact (/ (- new-max new-min)
                                     (- vmax vmin)))])
      (for/vector #:length (vector-length vect) ([v (in-vector vect)])
        (fl->exact-integer
          (flround (fl+ (fl* (->fl (- v vmin)) factor)
                        (->fl new-min))))))))

(define (unmasked-index argb-bytes)
  (for/vector ([i (in-range 1 (bytes-length argb-bytes) 4)]
               #:unless (eq? (bytes-ref argb-bytes i) mask-val))
    i))

;; Returns modified bytes representing colorized image.
(define (colorize-image-bytes! argb-bytes)
  (let* ([idx (unmasked-index argb-bytes)]
         [gs (for/vector #:length (vector-length idx)
               ([i (in-vector idx)])
               (bytes-ref argb-bytes i))]
         [vs (vector-stretch gs)]
         [ncolors (sub1 (vector-length colors-haxby))])
    (for ([i (in-vector idx)]
          [v (in-vector vs)])
      (bytes-copy! argb-bytes i
                   (list->bytes (vector-ref colors-haxby (min v ncolors)))))))

;; Colorize a bitmap object from grayscale values.
(define (colorize-bitmap! bitmap)
  (let* ([w (send bitmap get-width)]
         [h (send bitmap get-height)]
         [argb-bytes (make-bytes (* 4 w h))])
    (send bitmap get-argb-pixels 0 0 w h argb-bytes)
    (colorize-image-bytes! argb-bytes)
    (send bitmap set-argb-pixels 0 0 w h argb-bytes)))
