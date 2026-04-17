(ns bitool.logic
  (:require [clojure.string :as string]))

(def ^:private identifier-pattern #"^[A-Za-z_][A-Za-z0-9_]*$")

(defn valid-identifier?
  [value]
  (boolean
   (and (string? value)
        (re-matches identifier-pattern value))))

(defn- non-blank
  [value]
  (let [trimmed (some-> value str string/trim)]
    (when (seq trimmed) trimmed)))

(defn- parse-number-literal
  [token]
  (if (re-find #"\." token)
    (Double/parseDouble token)
    (Long/parseLong token)))

(defn- decode-string-literal
  [token]
  (let [inner (subs token 1 (dec (count token)))]
    (loop [chars (seq inner)
           out   (StringBuilder.)]
      (if-let [ch (first chars)]
        (if (= ch \\)
          (let [next-ch (second chars)]
            (when-not next-ch
              (throw (ex-info "Invalid string literal"
                              {:field :expression
                               :token token})))
            (.append out
                     (case next-ch
                       \\ \\
                       \" \"
                       \' \'
                       \n \newline
                       \r \return
                       \t \tab
                       next-ch))
            (recur (nnext chars) out))
          (do
            (.append out ch)
            (recur (next chars) out)))
        (.toString out)))))

(defn- tokenize
  [expression]
  (let [expr (or expression "")
        len  (count expr)]
    (loop [idx 0
           tokens []]
      (if (>= idx len)
        (conj tokens {:type :eof})
        (let [ch  (.charAt expr idx)
              two (when (< (inc idx) len)
                    (subs expr idx (+ idx 2)))]
          (cond
            (Character/isWhitespace ch)
            (recur (inc idx) tokens)

            (#{"&&" "||" "==" "!=" "<=" ">=" "<>"} two)
            (recur (+ idx 2)
                   (conj tokens {:type :operator :value two}))

            (#{\( \) \, \+ \- \* \/ \% \! \< \> \=} ch)
            (recur (inc idx)
                   (conj tokens {:type (case ch
                                         \( :lparen
                                         \) :rparen
                                         \, :comma
                                         :operator)
                                 :value (str ch)}))

            (or (= ch \") (= ch \'))
            (let [quote-char ch
                  end (loop [j (inc idx)
                             escaped? false]
                        (when (>= j len)
                          (throw (ex-info "Unterminated string literal"
                                          {:field :expression
                                           :expression expr})))
                        (let [current (.charAt expr j)]
                          (cond
                            escaped?
                            (recur (inc j) false)

                            (= current \\)
                            (recur (inc j) true)

                            (= current quote-char)
                            j

                            :else
                            (recur (inc j) false))))
                  token (subs expr idx (inc end))]
              (recur (inc end)
                     (conj tokens {:type :string
                                   :value (decode-string-literal token)})))

            (or (Character/isDigit ch)
                (and (= ch \.)
                     (< (inc idx) len)
                     (Character/isDigit (.charAt expr (inc idx)))))
            (let [end (loop [j idx
                             seen-dot? false]
                        (if (>= j len)
                          j
                          (let [current (.charAt expr j)]
                            (cond
                              (Character/isDigit current)
                              (recur (inc j) seen-dot?)

                              (and (= current \.) (not seen-dot?))
                              (recur (inc j) true)

                              :else
                              j))))
                  token (subs expr idx end)]
              (recur end
                     (conj tokens {:type :number
                                   :value (parse-number-literal token)})))

            (or (Character/isLetter ch) (= ch \_))
            (let [end (loop [j idx]
                        (if (>= j len)
                          j
                          (let [current (.charAt expr j)]
                            (if (or (Character/isLetterOrDigit current)
                                    (= current \_))
                              (recur (inc j))
                              j))))
                  token (subs expr idx end)
                  lower (string/lower-case token)]
              (recur end
                     (conj tokens
                           (case lower
                             "true"  {:type :boolean :value true}
                             "false" {:type :boolean :value false}
                             "null"  {:type :null :value nil}
                             "and"   {:type :operator :value "&&"}
                             "or"    {:type :operator :value "||"}
                             "not"   {:type :operator :value "!"}
                             {:type :identifier :value token}))))

            :else
            (throw (ex-info "Invalid character in expression"
                            {:field :expression
                             :expression expr
                             :character (str ch)
                             :index idx}))))))))

(defn parse-expression
  [expression]
  (let [tokens (tokenize expression)
        cursor (atom 0)
        current-token #(nth tokens @cursor {:type :eof})
        consume! (fn []
                   (let [token (current-token)]
                     (swap! cursor inc)
                     token))
        match-token? (fn [type value]
                       (let [token (current-token)]
                         (and (= type (:type token))
                              (= value (:value token)))))
        expect! (fn [type value message]
                  (if (match-token? type value)
                    (consume!)
                    (throw (ex-info message
                                    {:field :expression
                                     :expression expression
                                     :token (current-token)}))))]
    (letfn [(parse-call [fn-name]
              (consume!)
              (loop [args []]
                (if (match-token? :rparen ")")
                  (do
                    (consume!)
                    {:type :call :name fn-name :args args})
                  (let [arg (parse-or)]
                    (if (match-token? :comma ",")
                      (do
                        (consume!)
                        (recur (conj args arg)))
                      (do
                        (expect! :rparen ")" "Missing closing ')' in function call")
                        {:type :call :name fn-name :args (conj args arg)}))))))

            (parse-primary []
              (let [{:keys [type value] :as token} (current-token)]
                (case type
                  :number
                  (do
                    (consume!)
                    {:type :literal :value value})

                  :string
                  (do
                    (consume!)
                    {:type :literal :value value})

                  :boolean
                  (do
                    (consume!)
                    {:type :literal :value value})

                  :null
                  (do
                    (consume!)
                    {:type :literal :value nil})

                  :identifier
                  (do
                    (consume!)
                    (if (match-token? :lparen "(")
                      (parse-call value)
                      {:type :var :name value}))

                  :lparen
                  (do
                    (consume!)
                    (let [node (parse-or)]
                      (expect! :rparen ")" "Missing closing ')'")
                      node))

                  (throw (ex-info "Unexpected token in expression"
                                  {:field :expression
                                   :expression expression
                                   :token token})))))

            (parse-unary []
              (if-let [{:keys [value]} (when (= :operator (:type (current-token)))
                                         (current-token))]
                (if (#{"-" "!"} value)
                  (do
                    (consume!)
                    {:type :unary
                     :op value
                     :expr (parse-unary)})
                  (parse-primary))
                (parse-primary)))

            (parse-multiplicative []
              (loop [left (parse-unary)]
                (if-let [{:keys [value]} (when (and (= :operator (:type (current-token)))
                                                    (#{"*" "/" "%"} (:value (current-token))))
                                           (current-token))]
                  (do
                    (consume!)
                    (recur {:type :binary
                            :op value
                            :left left
                            :right (parse-unary)}))
                  left)))

            (parse-additive []
              (loop [left (parse-multiplicative)]
                (if-let [{:keys [value]} (when (and (= :operator (:type (current-token)))
                                                    (#{"+" "-"} (:value (current-token))))
                                           (current-token))]
                  (do
                    (consume!)
                    (recur {:type :binary
                            :op value
                            :left left
                            :right (parse-multiplicative)}))
                  left)))

            (parse-comparison []
              (loop [left (parse-additive)]
                (if-let [{:keys [value]} (when (and (= :operator (:type (current-token)))
                                                    (#{"<" "<=" ">" ">="} (:value (current-token))))
                                           (current-token))]
                  (do
                    (consume!)
                    (recur {:type :binary
                            :op value
                            :left left
                            :right (parse-additive)}))
                  left)))

            (parse-equality []
              (loop [left (parse-comparison)]
                (if-let [{:keys [value]} (when (and (= :operator (:type (current-token)))
                                                    (#{"==" "!=" "=" "<>"} (:value (current-token))))
                                           (current-token))]
                  (do
                    (consume!)
                    (recur {:type :binary
                            :op value
                            :left left
                            :right (parse-comparison)}))
                  left)))

            (parse-and []
              (loop [left (parse-equality)]
                (if (match-token? :operator "&&")
                  (do
                    (consume!)
                    (recur {:type :binary
                            :op "&&"
                            :left left
                            :right (parse-equality)}))
                  left)))

            (parse-or []
              (loop [left (parse-and)]
                (if (match-token? :operator "||")
                  (do
                    (consume!)
                    (recur {:type :binary
                            :op "||"
                            :left left
                            :right (parse-and)}))
                  left)))]
      (let [ast (parse-or)]
        (when-not (= :eof (:type (current-token)))
          (throw (ex-info "Unexpected trailing tokens in expression"
                          {:field :expression
                           :expression expression
                           :token (current-token)})))
        ast))))

(defn- maybe-number
  [value]
  (cond
    (number? value) value
    (true? value) 1
    (false? value) 0
    (string? value)
    (let [trimmed (string/trim value)]
      (when (seq trimmed)
        (try
          (if (re-find #"\." trimmed)
            (Double/parseDouble trimmed)
            (Long/parseLong trimmed))
          (catch Exception _
            nil))))
    :else nil))

(defn- number-value
  [value]
  (or (maybe-number value)
      (throw (ex-info "Expected a numeric value"
                      {:field :expression
                       :value value}))))

(defn- integer-value
  [value]
  (int (Math/floor (double (number-value value)))))

(defn- truthy?
  [value]
  (cond
    (nil? value) false
    (false? value) false
    (string? value) (not (string/blank? value))
    (coll? value) (boolean (seq value))
    :else true))

(defn- flatten-args
  [args]
  (mapcat #(if (and (sequential? %) (not (string? %))) % [%]) args))

(def ^:private built-in-functions
  {"abs"        (fn [x] (Math/abs (double (number-value x))))
   "average"    (fn [& xs]
                  (let [values (map number-value (flatten-args xs))]
                    (if (seq values)
                      (/ (reduce + values) (double (count values)))
                      0.0)))
   "ceil"       (fn [x] (Math/ceil (double (number-value x))))
   "coalesce"   (fn [& xs] (first (drop-while nil? xs)))
   "concat"     (fn [& xs] (apply str (map #(or % "") xs)))
   "contains"   (fn [value search]
                  (string/includes? (str (or value "")) (str (or search ""))))
   "cos"        (fn [x] (Math/cos (double (number-value x))))
   "endswith"   (fn [value suffix]
                  (string/ends-with? (str (or value "")) (str (or suffix ""))))
   "equals"     (fn [a b] (= a b))
   "exp"        (fn [x] (Math/exp (double (number-value x))))
   "floor"      (fn [x] (Math/floor (double (number-value x))))
   "if"         (fn [condition when-true when-false]
                  (if (truthy? condition) when-true when-false))
   "indexof"    (fn [value search]
                  (.indexOf (str (or value "")) (str (or search ""))))
   "isempty"    (fn [value]
                  (or (nil? value)
                      (and (string? value) (string/blank? value))
                      (and (coll? value) (empty? value))))
   "isnull"     nil?
   "length"     (fn [value]
                  (cond
                    (nil? value) 0
                    (string? value) (count value)
                    (coll? value) (count value)
                    :else (count (str value))))
   "log"        (fn [x] (Math/log (double (number-value x))))
   "log10"      (fn [x] (Math/log10 (double (number-value x))))
   "lower"      (fn [value] (string/lower-case (str (or value ""))))
   "max"        (fn [& xs]
                  (reduce max (map #(double (number-value %)) (flatten-args xs))))
   "matches"    (fn [value pattern]
                  (boolean (re-find (re-pattern (str pattern)) (str (or value "")))))
   "min"        (fn [& xs]
                  (reduce min (map #(double (number-value %)) (flatten-args xs))))
   "not"        (fn [value] (not (truthy? value)))
   "or"         (fn [& xs] (boolean (some truthy? xs)))
   "and"        (fn [& xs] (every? truthy? xs))
   "parsefloat" (fn [value] (Double/parseDouble (str value)))
   "parseint"   (fn [value] (Long/parseLong (str value)))
   "pow"        (fn [x y] (Math/pow (double (number-value x))
                                    (double (number-value y))))
   "replace"    (fn [value search replacement]
                  (string/replace (str (or value ""))
                                  (java.util.regex.Pattern/quote (str (or search "")))
                                  (java.util.regex.Matcher/quoteReplacement (str (or replacement "")))))
   "round"      (fn
                  ([x] (Math/round (double (number-value x))))
                  ([x digits]
                   (let [factor (Math/pow 10 (double (integer-value digits)))]
                     (/ (Math/round (* (double (number-value x)) factor))
                        factor))))
   "sin"        (fn [x] (Math/sin (double (number-value x))))
   "sqrt"       (fn [x] (Math/sqrt (double (number-value x))))
   "startswith" (fn [value prefix]
                  (string/starts-with? (str (or value "")) (str (or prefix ""))))
   "substring"  (fn
                  ([value start]
                   (subs (str (or value "")) (integer-value start)))
                  ([value start end]
                   (subs (str (or value ""))
                         (integer-value start)
                         (integer-value end))))
   "sum"        (fn [& xs]
                  (reduce + 0 (map number-value (flatten-args xs))))
   "tan"        (fn [x] (Math/tan (double (number-value x))))
   "tofixed"    (fn [value digits]
                  (format (str "%." (integer-value digits) "f")
                          (double (number-value value))))
   "toboolean"  truthy?
   "tolowercase" (fn [value] (string/lower-case (str (or value ""))))
   "tonumber"   number-value
   "tostring"   (fn [value] (if (nil? value) "" (str value)))
   "touppercase" (fn [value] (string/upper-case (str (or value ""))))
   "trim"       (fn [value] (string/trim (str (or value ""))))
   "upper"      (fn [value] (string/upper-case (str (or value ""))))})

(declare evaluate-ast)

(defn- compare-values
  [left right comparator]
  (let [left-num  (maybe-number left)
        right-num (maybe-number right)]
    (if (and (some? left-num) (some? right-num))
      (comparator (double left-num) (double right-num))
      (comparator (str (or left "")) (str (or right ""))))))

(defn- evaluate-binary
  [op left right]
  (case op
    "+"  (if (or (string? left) (string? right))
           (str (or left "") (or right ""))
           (+ (number-value left) (number-value right)))
    "-"  (- (number-value left) (number-value right))
    "*"  (* (number-value left) (number-value right))
    "/"  (/ (double (number-value left)) (double (number-value right)))
    "%"  (mod (long (number-value left)) (long (number-value right)))
    "==" (= left right)
    "="  (= left right)
    "!=" (not= left right)
    "<>" (not= left right)
    "<"  (compare-values left right <)
    "<=" (compare-values left right <=)
    ">"  (compare-values left right >)
    ">=" (compare-values left right >=)
    "&&" (and (truthy? left) (truthy? right))
    "||" (or (truthy? left) (truthy? right))
    (throw (ex-info "Unsupported operator"
                    {:field :expression
                     :operator op}))))

(defn evaluate-ast
  [ast env]
  (case (:type ast)
    :literal (:value ast)
    :var
    (if (contains? env (:name ast))
      (get env (:name ast))
      (throw (ex-info "Unknown variable in logic expression"
                      {:field :expression
                       :variable (:name ast)})))
    :unary
    (let [value (evaluate-ast (:expr ast) env)]
      (case (:op ast)
        "-" (- (number-value value))
        "!" (not (truthy? value))
        (throw (ex-info "Unsupported unary operator"
                        {:field :expression
                         :operator (:op ast)}))))
    :binary
    (let [left-value (evaluate-ast (:left ast) env)]
      (case (:op ast)
        "&&" (if (truthy? left-value)
               (truthy? (evaluate-ast (:right ast) env))
               false)
        "||" (if (truthy? left-value)
               true
               (truthy? (evaluate-ast (:right ast) env)))
        (evaluate-binary (:op ast)
                         left-value
                         (evaluate-ast (:right ast) env))))
    :call
    (let [fn-name (string/lower-case (:name ast))
          fn-body (get built-in-functions fn-name)
          args    (map #(evaluate-ast % env) (:args ast))]
      (when-not fn-body
        (throw (ex-info "Unknown logic function"
                        {:field :expression
                         :function (:name ast)})))
      (apply fn-body args))
    (throw (ex-info "Unsupported AST node"
                    {:field :expression
                     :ast ast}))))

(defn evaluate-expression
  [expression env]
  (-> expression
      parse-expression
      (evaluate-ast env)))

(defn- output-expression
  [output fn-return output-count]
  (or (non-blank (:expression output))
      (when (and (= output-count 1)
                 (non-blank fn-return))
        (non-blank fn-return))))

(defn- duplicate-items
  [values]
  (->> values
       frequencies
       (keep (fn [[value count]]
               (when (> count 1) value)))
       vec))

(defn validate-logic-config!
  [{:keys [fn_params fn_lets fn_return fn_outputs] :as logic-node}]
  (let [param-names (keep (comp non-blank :param_name) fn_params)
        let-names   (keep (comp non-blank :variable) fn_lets)
        output-names (keep (comp non-blank :output_name) fn_outputs)
        duplicates  (vec (concat (duplicate-items param-names)
                                 (duplicate-items let-names)
                                 (duplicate-items output-names)))]
    (doseq [param fn_params]
      (when-let [name (non-blank (:param_name param))]
        (when-not (valid-identifier? name)
          (throw (ex-info "Parameter names must be valid identifiers"
                          {:field :fn_params
                           :value name})))))
    (doseq [let-row fn_lets]
      (let [name (non-blank (:variable let-row))
            expr (non-blank (:expression let-row))]
        (when-not (valid-identifier? name)
          (throw (ex-info "Assignment variable names must be valid identifiers"
                          {:field :fn_lets
                           :value name})))
        (when-not expr
          (throw (ex-info "Assignment expressions must not be blank"
                          {:field :fn_lets
                           :value let-row})))
        (parse-expression expr)))
    (doseq [output fn_outputs]
      (let [name (non-blank (:output_name output))
            expr (output-expression output fn_return (count fn_outputs))]
        (when-not (valid-identifier? name)
          (throw (ex-info "Output names must be valid identifiers"
                          {:field :fn_outputs
                           :value name})))
        (when-not expr
          (throw (ex-info "Each output needs a return expression"
                          {:field :fn_outputs
                           :value output})))
        (parse-expression expr)))
    (when (seq duplicates)
      (throw (ex-info "Logic node names must be unique"
                      {:field :logic
                       :duplicates duplicates})))
    logic-node))

(defn- lookup-flat-value
  [flat key-name]
  (let [missing (Object.)
        direct  (get flat key-name missing)]
    (if (not (identical? direct missing))
      direct
      (or (some (fn [[k v]]
                  (when (= (string/lower-case (str k))
                           (string/lower-case key-name))
                    v))
                flat)
          nil))))

(defn- base-env
  [flat]
  (reduce-kv (fn [acc key value]
               (let [name (str key)]
                 (if (valid-identifier? name)
                   (assoc acc name value)
                   acc)))
             {}
             flat))

(defn- qualified-outputs
  [node outputs]
  (if-let [stage-name (non-blank (:name node))]
    (reduce-kv (fn [acc key value]
                 (assoc acc (str stage-name "." key) value))
               {}
               outputs)
    {}))

(defn execute-logic-node
  [{:keys [fn_params fn_lets fn_return fn_outputs] :as logic-node} flat]
  (validate-logic-config! logic-node)
  (let [env-with-params
        (reduce (fn [env {:keys [param_name source_column]}]
                  (let [name   (non-blank param_name)
                        source (or (non-blank source_column) name)]
                    (if name
                      (assoc env name (lookup-flat-value flat source))
                      env)))
                (base-env flat)
                fn_params)
        env-after-lets
        (reduce (fn [env {:keys [variable expression]}]
                  (assoc env
                         (non-blank variable)
                         (evaluate-expression expression env)))
                env-with-params
                fn_lets)
        [final-env outputs]
        (reduce (fn [[env out] output]
                  (let [name  (non-blank (:output_name output))
                        expr  (output-expression output fn_return (count fn_outputs))
                        value (evaluate-expression expr env)]
                    [(assoc env name value)
                     (assoc out name value)]))
                [env-after-lets {}]
                fn_outputs)
        outputs
        (if (seq outputs)
          outputs
          (if-let [expr (non-blank fn_return)]
            {"result" (evaluate-expression expr final-env)}
            {}))]
    (merge flat outputs (qualified-outputs logic-node outputs))))

(def ^:private conditional-types
  #{"if-else" "if-elif-else" "multi-if" "case" "cond" "pattern-match"})

(defn conditional-output-prefix
  [{:keys [id]}]
  (str "cond_" (or id "node")))

(defn conditional-outputs->tcols
  [node-id]
  (let [prefix (conditional-output-prefix {:id node-id})]
    {node-id [{:column_name (str prefix "_group")
               :data_type "varchar"
               :is_nullable "YES"}
              {:column_name (str prefix "_matched")
               :data_type "boolean"
               :is_nullable "YES"}
              {:column_name (str prefix "_used_default")
               :data_type "boolean"
               :is_nullable "YES"}
              {:column_name (str prefix "_branch_index")
               :data_type "integer"
               :is_nullable "YES"}
              {:column_name (str prefix "_condition")
               :data_type "varchar"
               :is_nullable "YES"}
              {:column_name (str prefix "_value")
               :data_type "varchar"
               :is_nullable "YES"}]}))

(defn- meaningful-conditional-branch?
  [branch]
  (some non-blank
        [(:condition branch)
         (:guard branch)
         (:group branch)
         (:value branch)]))

(defn- normalize-conditional-branch
  [branch]
  (let [normalized (->> (or branch {})
                        (map (fn [[k v]] [(keyword k) v]))
                        (keep (fn [[k v]]
                                (when-let [trimmed (non-blank v)]
                                  [k trimmed])))
                        (into {}))]
    (when (meaningful-conditional-branch? normalized)
      normalized)))

(defn- branch-expression
  [cond-type branch]
  (case cond-type
    "pattern-match" (:guard branch)
    (:condition branch)))

(defn validate-conditional-config!
  [{:keys [cond_type branches default_branch headers] :as conditional-node}]
  (let [cond-type         (some-> (or cond_type "if-else") str string/trim string/lower-case)
        normalized-branches (->> (or branches [])
                                 (mapv normalize-conditional-branch)
                                 (remove nil?)
                                 vec)
        normalized-headers  (->> (or headers [])
                                 (keep non-blank)
                                 vec)
        normalized-node   (assoc conditional-node
                                 :cond_type cond-type
                                 :branches normalized-branches
                                 :default_branch (or (non-blank default_branch) "")
                                 :headers normalized-headers)]
    (when-not (contains? conditional-types cond-type)
      (throw (ex-info "Invalid conditional type"
                      {:field :cond_type
                       :value cond-type})))
    (when-not (seq normalized-branches)
      (throw (ex-info "Conditional nodes need at least one branch"
                      {:field :branches})))
    (doseq [branch normalized-branches]
      (let [expr  (branch-expression cond-type branch)
            group (non-blank (:group branch))]
        (when-not expr
          (throw (ex-info "Each conditional branch needs a condition or guard"
                          {:field :branches
                           :value branch})))
        (when-not group
          (throw (ex-info "Each conditional branch needs a result group"
                          {:field :branches
                           :value branch})))
        (parse-expression expr)
        (when (and (= cond-type "case")
                   (nil? (non-blank (:value branch))))
          (throw (ex-info "Case branches need a match value"
                          {:field :branches
                           :value branch})))))
    normalized-node))

(defn- values-equal?
  [left right]
  (let [left-num  (maybe-number left)
        right-num (maybe-number right)]
    (if (and (some? left-num) (some? right-num))
      (= (double left-num) (double right-num))
      (= left right))))

(defn- case-value-literal?
  [value]
  (boolean
   (and (string? value)
        (not (re-find #"[\s()'\"+\-*/%<>=!,]" value))
        (not (re-matches #"(?i:true|false|null)" value))
        (not (re-matches #"-?\d+(\.\d+)?" value)))))

(defn- evaluate-case-value
  [value env]
  (let [trimmed (non-blank value)]
    (cond
      (nil? trimmed) nil
      (and (valid-identifier? trimmed) (contains? env trimmed))
      (get env trimmed)
      (case-value-literal? trimmed)
      trimmed
      :else
      (evaluate-expression trimmed env))))

(defn- predicate-branch-match
  [cond-type branch env]
  (let [expr (branch-expression cond-type branch)]
    (when (truthy? (evaluate-expression expr env))
      {:matched? true
       :group (:group branch)
       :branch-index nil
       :condition expr
       :value (:value branch)})))

(defn- case-branch-match
  [branch env]
  (let [subject (evaluate-expression (:condition branch) env)
        value   (evaluate-case-value (:value branch) env)]
    (when (values-equal? subject value)
      {:matched? true
       :group (:group branch)
       :branch-index nil
       :condition (:condition branch)
       :value value})))

(defn- conditional-selection
  [{:keys [cond_type branches default_branch]} env]
  (or
   (some identity
         (map-indexed
          (fn [idx branch]
            (when-let [match (if (= cond_type "case")
                               (case-branch-match branch env)
                               (predicate-branch-match cond_type branch env))]
              (assoc match :branch-index idx)))
          branches))
   {:matched? false
    :group default_branch
    :branch-index nil
    :condition nil
    :value nil}))

(defn execute-conditional-node
  [conditional-node flat]
  (let [{:keys [cond_type branches default_branch] :as normalized}
        (validate-conditional-config! conditional-node)
        env     (base-env flat)
        prefix  (conditional-output-prefix normalized)
        {:keys [matched? group branch-index condition value]}
        (conditional-selection {:cond_type cond_type
                                :branches branches
                                :default_branch default_branch}
                               env)
        outputs {(str prefix "_group") group
                 (str prefix "_matched") matched?
                 (str prefix "_used_default") (and (not matched?)
                                                   (boolean (non-blank default_branch)))
                 (str prefix "_branch_index") branch-index
                 (str prefix "_condition") condition
                 (str prefix "_value") value}]
    (merge flat
           outputs
           (when-let [stage-name (non-blank (:name normalized))]
             {(str stage-name ".group") group
              (str stage-name ".matched") matched?
              (str stage-name ".used_default") (and (not matched?)
                                                    (boolean (non-blank default_branch)))
              (str stage-name ".branch_index") branch-index
              (str stage-name ".condition") condition
              (str stage-name ".value") value}))))
