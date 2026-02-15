(ns bitool.macros)

;; =============================================================================
;; ANAPHORIC MACROS (On Lisp Ch. 14)
;; =============================================================================

(defmacro aif
  "Anaphoric if. Binds test result to 'it' in then/else clauses.
  
  Example:
    (aif (get-config :timeout)
         (set-timeout it)
         (use-default))"
  ([test then]
   `(aif ~test ~then nil))
  ([test then else]
   `(let [~'it ~test]
      (if ~'it ~then ~else))))

(defmacro awhen
  "Anaphoric when. Binds test result to 'it' in body.
  
  Example:
    (awhen (fetch-include node)
           (process it)
           (cache it))"
  [test & body]
  `(aif ~test
        (do ~@body)))

(defmacro awhile
  "Anaphoric while. Binds test result to 'it' in body.
  
  Example:
    (awhile (next-batch)
            (process-batch it))"
  [test & body]
  `(loop []
     (awhen ~test
            ~@body
            (recur))))

(defmacro acond
  "Anaphoric cond. Each test result bound to 'it'.
  
  Example:
    (acond
      (getk job \"script\") (process-script it)
      (getk job \"trigger\") (process-trigger it)
      :else (error \"no action\"))"
  [& clauses]
  (if (empty? clauses)
    nil
    (let [[test expr & rest] clauses]
      (if (= test :else)
        expr
        `(aif ~test
              ~expr
              (acond ~@rest))))))

(defmacro prn-v [sym]
  `(println (str ~(name sym) " " ~sym)))

(defmacro anil? [x y]
  (let [g (gensym "val")]
    `(let [~g ~x]
       (if (nil? ~g) ~y ~g))))

(defmacro aassoc-in
  "Like assoc-in, but only associates the value if it's not nil.
   The value expression is bound to 'it' for anaphoric reference."
  [m path value-expr]
  `(let [~'it ~value-expr]
     (if (some? ~'it)
       (assoc-in ~m ~path ~'it)
       ~m)))

(defmacro aassoc-in->
  "Thread-first version that chains multiple assoc-in-some operations."
  [m & clauses]
  (reduce (fn [acc [path value-expr]]
            `(aassoc-in ~acc ~path ~value-expr))
          m
          (partition 2 clauses)))

(defmacro ->args [args & forms]
  (let [g-sym (gensym "g-")
        rest-args (gensym "rest-")]
    `(let [[~g-sym & ~rest-args] ~args]
       ~(reduce (fn [acc form]
                  `(apply ~form ~acc ~rest-args))
                g-sym
                forms))))

(aassoc-in-> {:a 1}
  [:b :c] 42
  [:d :e] nil
  [:f :g] "hello")
;; => {:a 1, :b {:c 42}, :f {:g "hello}}

;; Basic usage
(aassoc-in {:a 1} [:b :c] 42)
;; => {:a 1, :b {:c 42}}

(aassoc-in {:a 1} [:b :c] nil)
;; => {:a 1}

;; Anaphoric feature - 'it' refers to the value
;; (aassoc-in {:a 1} [:b :c] (when (pos? 5) it))
;; => {:a 1}  (because 'it' is nil in the macro context)

;; More useful anaphoric example
(let [user {:name "Alice"}]
  (aassoc-in user [:profile :email]
                 (some-> user :email-raw clojure.string/lower-case)))

(defmacro defpreds [& pairs]
  `(do
     ~@(for [[name type-str] (partition 2 pairs)]
         `(defmacro ~name [] '(= ~'btype ~type-str)))))

;; Define them all at once
(defpreds
  mapping "Mp"
  target  "Tg"
  output  "Op"
  property "Pr"
  block    "Bl")

;; Usage
;; (if (mapping) ...)
;; (if (target) ...)
;; (if (output) ...)

(defmacro if=
  [a b then-expr else-expr]
  `(let [a# ~a
         b# ~b]
     (if (= a# b#)
       ~then-expr
       ~else-expr)))

(defmacro cond= [x & clauses]
  (let [x# (gensym "x__")]
    `(let [~x# ~x]
       (cond
         ~@(mapcat (fn [[v expr]]
                     [`(= ~x# ~v) expr])
                   (partition 2 clauses))))))


