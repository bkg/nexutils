#lang racket/base

(require racket/class
         racket/draw
         racket/function
         racket/list
         racket/match
         racket/math
         racket/sequence
         racket/vector)

(provide (all-defined-out))

(define (quantiles seq plst)
  (let* ([vsort (list->vector (sort (sequence->list seq) <))]
         [vlen (vector-length vsort)])
    (define (quantile p) (vector-ref vsort (exact-ceiling (sub1 (* p vlen)))))
    (map quantile plst)))

(define (limit v low high) (min (max v low) high))

(define (vector-normalize vect [vmin/max #f] [nmin/max '(0 255)])
  (match-let ([(list vmin vmax) (or vmin/max
                                    (list (vector-argmin identity vect)
                                          (vector-argmax identity vect)))]
              [(list new-min new-max) nmin/max])
    (let ([range-scale (exact->inexact (/ (- new-max new-min) (- vmax vmin)))])
    (for/vector ([v (in-vector vect)])
      (limit (exact-floor (* (exact->inexact (- v vmin)) range-scale))
             new-min
             new-max)))))

(define (interpolate-color x c1 c2)
  (for/list ([ch1 (in-list c1)]
             [ch2 (in-list c2)])
    (limit (floor (+ ch1 (* x (- ch2 ch1)))) 0 255)))

(define (linear-gradient start finish [n 10])
  (for/list ([t (in-range n)])
    (interpolate-color (/ t (sub1 n)) start finish)))

(define (multilinear-gradient colors [n (* (length colors) 10)])
  (let ([steps (/ n (sub1 (length colors)))])
    (cons (car colors)
          (append-map cdr
                      (for/list ([c1 (in-list colors)]
                                 [c2 (in-list (cdr colors))])
                        (linear-gradient c1 c2 steps))))))

(define colors-haxby
  (list->vector
    (multilinear-gradient '((10 0 121)
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

(define (colorize-image-bytes! argb-bytes)
  (let* ([gray-vect (for/vector ([b (in-bytes argb-bytes 1 #f 4)]
                                 #:unless (eq? b 255)) b)]
         [q (quantiles (in-vector gray-vect 0 #f 8) '(.02 .98))]
         [red-indices (for/vector ([i (in-range 1 (bytes-length argb-bytes) 4)]
                                   #:unless (eq? (bytes-ref argb-bytes i) 255)) i)]
         [ncolors (sub1 (vector-length colors-haxby))])
    (for ([i (in-vector red-indices)]
          [v (in-vector (vector-normalize gray-vect q '(0 254)))])
      (bytes-copy! argb-bytes i
                   (list->bytes (vector-ref colors-haxby (min v ncolors)))))))

(define (colorize-bitmap bitmap)
  (let* ([w (send bitmap get-width)]
         [h (send bitmap get-height)]
         [argb-bytes (make-bytes (* 4 w h))])
    (send bitmap get-argb-pixels 0 0 w h argb-bytes)
    (colorize-image-bytes! argb-bytes)
    (send bitmap set-argb-pixels 0 0 w h argb-bytes)))