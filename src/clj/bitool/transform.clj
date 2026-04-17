(ns bitool.transform
  "Natural language -> constrained transform GIL for graph generation.
   Uses an LLM when available, with deterministic fallback templates so the
   feature still works locally and in tests."
  (:require [bitool.ai.llm :as llm]
            [bitool.gil.normalize :as gil-normalize]
            [bitool.gil.validator :as gil-validator]
            [bitool.logic :as logic]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [clojure.set :as set]
            [clojure.tools.logging :as log]))

(def ^:private allowed-node-types
  #{"endpoint" "table" "function" "conditionals" "response-builder" "output"})

(def ^:private allowed-stage-kinds
  #{"logic" "conditional"})

(def ^:private allowed-source-kinds
  #{"endpoint" "table"})

(def ^:private allowed-conditional-outputs
  #{"group" "matched" "used_default" "branch_index" "condition" "value"})

(def ^:private recommended-functions
  ["trim" "upper" "lower" "concat" "tonumber" "toboolean" "tostring"
   "coalesce" "contains" "startswith" "endswith" "replace" "round"
   "length" "if" "isnull" "isempty" "min" "max" "sum"])

(declare semantic-plan->gil validate-transform-gil)

(def ^:private max-repair-attempts 1)

(def ^:private transform-tool
  {:name "create_transform_graph"
   :description "Create a transform graph specification from natural language. Return only the transform spec."
   :input_schema
   {:type "object"
    :required ["graph_name" "source" "stages"]
    :properties
    {:graph_name {:type "string"}
     :goal {:type "string"}
     :source {:type "object"
              :required ["kind"]
              :properties
              {:kind {:type "string" :enum ["endpoint" "table"]}
               :http_method {:type "string" :enum ["GET" "POST" "PUT" "DELETE" "PATCH"]}
               :route_path {:type "string"}
               :query_params {:type "array" :items {:type "string"}}
               :table_name {:type "string"}}}
     :stages {:type "array"
              :items {:type "object"
                      :required ["kind" "name"]
                      :properties
                      {:kind {:type "string" :enum ["logic" "conditional"]}
                       :name {:type "string"}
                       :inputs {:type "object"
                                :additionalProperties {:type "string"}}
                       :assignments {:type "array"
                                     :items {:type "object"
                                             :required ["var" "expr"]
                                             :properties {:var {:type "string"}
                                                          :expr {:type "string"}}}}
                       :outputs {:type "object"
                                 :additionalProperties {:type "string"}}
                       :cond_type {:type "string"
                                   :enum ["if-else" "if-elif-else" "multi-if" "case" "cond" "pattern-match"]}
                       :branches {:type "array"
                                  :items {:type "object"
                                          :properties {:when {:type "string"}
                                                       :guard {:type "string"}
                                                       :value {:type "string"}
                                                       :group {:type "string"}}}}
                       :default {:type "object"
                                 :properties {:group {:type "string"}}}}}}
     :response {:type "object"
                :properties
                {:kind {:type "string" :enum ["response-builder"]}
                 :template {:type "object"
                            :additionalProperties {:type "string"}}}}
     :assumptions {:type "array" :items {:type "string"}}
     :explanations {:type "array" :items {:type "string"}}}}})

(def ^:private transform-examples
  [{:nl "Create an API flow that normalizes customer name and amount, classifies VIP customers, and returns a discount and score."
    :spec {:graph_name "Customer Decision Flow"
           :goal "Normalize request data, classify customer, and compute outputs"
           :source {:kind "endpoint"
                    :http_method "GET"
                    :route_path "/ai/customer-decision"
                    :query_params ["name" "country" "amount" "vip"]}
           :stages [{:kind "logic"
                     :name "normalize_input"
                     :inputs {"customer_name_raw" "name"
                              "country_raw" "country"
                              "amount_raw" "amount"
                              "vip_raw" "vip"}
                     :assignments [{:var "customer_name" :expr "trim(customer_name_raw)"}
                                   {:var "country_norm" :expr "upper(country_raw)"}
                                   {:var "amount_num" :expr "tonumber(amount_raw)"}
                                   {:var "vip_flag" :expr "toboolean(vip_raw)"}]
                     :outputs {"customer_name" "customer_name"
                               "country_norm" "country_norm"
                               "amount_num" "amount_num"
                               "vip_flag" "vip_flag"}}
                    {:kind "conditional"
                     :name "customer_segment"
                     :cond_type "if-elif-else"
                     :branches [{:when "country_norm = 'INDIA' && amount_num >= 1000" :group "priority_india"}
                                {:when "vip_flag" :group "vip_customer"}]
                     :default {:group "standard"}}
                    {:kind "logic"
                     :name "offer_logic"
                     :inputs {"customer_name" "normalize_input.customer_name"
                              "amount_num" "normalize_input.amount_num"
                              "segment" "customer_segment.group"}
                     :assignments [{:var "discount" :expr "if(segment = 'priority_india', 0.20, if(segment = 'vip_customer', 0.15, 0.05))"}
                                   {:var "score" :expr "if(segment = 'priority_india', amount_num * 2, if(segment = 'vip_customer', amount_num * 1.5, amount_num))"}]
                     :outputs {"greeting" "concat('Hello ', customer_name)"
                               "segment" "segment"
                               "discount" "discount"
                               "score" "score"}}]
           :response {:kind "response-builder"
                      :template {"greeting" "greeting"
                                 "segment" "segment"
                                 "discountRate" "discount"
                                 "priorityScore" "score"}}
           :assumptions ["Use an endpoint-backed transform graph."
                         "Treat VIP as a boolean-like input."]}}
   {:nl "Build a lead scoring transform that routes executive leads to account executives."
    :spec {:graph_name "Lead Routing Flow"
           :goal "Score leads and route them to the right owner"
           :source {:kind "endpoint"
                    :http_method "GET"
                    :route_path "/ai/lead-routing"
                    :query_params ["name" "title" "region" "employees" "budget"]}
           :stages [{:kind "logic"
                     :name "score_input"
                     :inputs {"lead_name_raw" "name"
                              "title_raw" "title"
                              "region_raw" "region"
                              "employees_raw" "employees"
                              "budget_raw" "budget"}
                     :assignments [{:var "lead_name" :expr "trim(lead_name_raw)"}
                                   {:var "title_norm" :expr "upper(title_raw)"}
                                   {:var "region_norm" :expr "upper(region_raw)"}
                                   {:var "employees_num" :expr "tonumber(employees_raw)"}
                                   {:var "budget_num" :expr "tonumber(budget_raw)"}]
                     :outputs {"lead_name" "lead_name"
                               "title_norm" "title_norm"
                               "region_norm" "region_norm"
                               "employees_num" "employees_num"
                               "budget_num" "budget_num"}}
                    {:kind "conditional"
                     :name "lead_segment"
                     :cond_type "if-elif-else"
                     :branches [{:when "budget_num >= 100000 && employees_num >= 1000" :group "exec_priority"}
                                {:when "contains(title_norm, 'CHIEF')" :group "executive_title"}
                                {:when "region_norm = 'NA'" :group "regional_priority"}]
                     :default {:group "nurture"}}
                    {:kind "logic"
                     :name "action_projection"
                     :inputs {"lead_name" "score_input.lead_name"
                              "segment" "lead_segment.group"}
                     :assignments [{:var "recommended_owner" :expr "if(segment = 'exec_priority', 'Account Executive', if(segment = 'executive_title', 'Enterprise Sales', if(segment = 'regional_priority', 'Regional AE', 'Inside Sales')))"}
                                   {:var "next_step" :expr "if(segment = 'nurture', 'Send nurture email', 'Schedule discovery call')"}]
                     :outputs {"greeting" "concat('Hello ', lead_name)"
                               "segment" "segment"
                               "recommended_owner" "recommended_owner"
                               "next_step" "next_step"}}]
           :response {:kind "response-builder"
                      :template {"greeting" "greeting"
                                 "segment" "segment"
                                 "recommendedOwner" "recommended_owner"
                                 "nextStep" "next_step"}}}}])

(defn- tokenize [text]
  (set (remove string/blank?
               (string/split (string/lower-case (str text)) #"[\s/,_\-.]+"))))

(defn- jaccard-score [a b]
  (let [u (count (set/union a b))]
    (if (zero? u)
      0.0
      (double (/ (count (set/intersection a b)) u)))))

(defn- best-examples [text]
  (let [q (tokenize text)]
    (->> transform-examples
         (map (fn [ex] (assoc ex :score (jaccard-score q (tokenize (:nl ex))))))
         (sort-by :score >)
         (take 2)
         vec)))

(defn- examples->prompt [examples]
  (string/join
   "\n\n"
   (map (fn [{:keys [nl spec]}]
          (str "User: " nl "\n"
               "Tool call: " (json/generate-string spec)))
        examples)))

(defn- build-system-prompt [text]
  (str
   "CRITICAL: You MUST respond ONLY by calling the create_transform_graph tool.\n"
   "Do NOT return prose, markdown, or code fences.\n\n"
   "You are generating a constrained transform graph spec for Bitool.\n"
   "The graph must be linear and limited to these node families:\n"
   "- source: endpoint or table\n"
   "- logic stage\n"
   "- conditional stage\n"
   "- response-builder\n"
   "- output\n\n"
   "Rules:\n"
   "1. Prefer endpoint-backed graphs unless the user clearly asks for a table source.\n"
   "2. Use only these stage kinds: logic, conditional.\n"
   "3. Use only these functions in expressions when possible: "
   (string/join ", " recommended-functions) "\n"
   "4. Conditional outputs available downstream are exactly: group, matched, used_default, branch_index, condition, value.\n"
   "5. Stage references in source bindings may use semantic refs like normalize_input.amount_num or customer_segment.group.\n"
   "6. Keep graphs in one of these shapes:\n"
   "   Endpoint -> Function -> Conditional -> Function -> Response Builder -> Output\n"
   "   Endpoint -> Conditional -> Function -> Response Builder -> Output\n"
   "   Endpoint -> Function -> Function -> Response Builder -> Output\n"
   "7. Response template values must reference final output fields, not prose.\n"
   "8. Make reasonable assumptions instead of asking questions.\n\n"
   "Examples:\n"
   (examples->prompt (best-examples text))))

(defn- non-blank [value]
  (some-> value str string/trim not-empty))

(defn- sanitize-identifier [value fallback]
  (let [s (-> (or value fallback)
              str
              string/trim
              string/lower-case
              (string/replace #"[^a-z0-9]+" "_")
              (string/replace #"^_+" "")
              (string/replace #"_+$" ""))]
    (cond
      (re-matches #"^[a-z_][a-z0-9_]*$" s) s
      (seq s) (str "n_" s)
      :else fallback)))

(defn- titleize [value]
  (->> (string/split (str value) #"[_\s/-]+")
       (remove string/blank?)
       (map string/capitalize)
       (string/join " ")))

(defn- slugify [value]
  (-> (sanitize-identifier value "generated_transform")
      (string/replace #"_" "-")))

(defn- semantic-ref? [value]
  (boolean (and (string? value)
                (re-matches #"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$" value))))

(defn- ->config-key
  [k]
  (let [s (cond
            (keyword? k) (name k)
            (string? k) k
            :else (str k))]
    (-> s
        (string/replace "-" "_")
        (string/replace #"([a-z0-9])([A-Z])" "$1_$2")
        string/lower-case
        keyword)))

(defn- guess-domain [text]
  (let [lower (string/lower-case (or text ""))]
    (cond
      (re-find #"\b(invoice|payment|ap|vendor|po)\b" lower) :invoice
      (re-find #"\b(lead|account executive|ae|executive)\b" lower) :lead
      (re-find #"\b(mastering|master data|golden record|survivorship|steward(ship)?)\b" lower) :mastering
      (re-find #"\b(eligibility|underwrit|loan|application|risk score|credit score)\b" lower) :eligibility
      (re-find #"\b(customer|vip|segment)\b" lower) :customer
      :else :order)))

(defn- endpoint-source [graph-name params]
  {:kind "endpoint"
   :http_method "GET"
   :route_path (str "/ai/" (slugify graph-name))
   :query_params params})

(defn- order-plan [text]
  {:graph_name (if (re-find #"\bapproval\b" (string/lower-case text))
                 "Order Approval Flow"
                 "Customer Decision Flow")
   :goal "Normalize request data, classify the record, and compute a business response"
   :source (endpoint-source "customer-decision" ["name" "country" "amount" "vip"])
   :stages [{:kind "logic"
             :name "normalize_input"
             :inputs {"customer_name_raw" "name"
                      "country_raw" "country"
                      "amount_raw" "amount"
                      "vip_raw" "vip"}
             :assignments [{:var "customer_name" :expr "trim(customer_name_raw)"}
                           {:var "country_norm" :expr "upper(country_raw)"}
                           {:var "amount_num" :expr "tonumber(amount_raw)"}
                           {:var "vip_flag" :expr "toboolean(vip_raw)"}]
             :outputs {"customer_name" "customer_name"
                       "country_norm" "country_norm"
                       "amount_num" "amount_num"
                       "vip_flag" "vip_flag"}}
            {:kind "conditional"
             :name "customer_segment"
             :cond_type "if-elif-else"
             :branches [{:when "country_norm = 'INDIA' && amount_num >= 1000" :group "priority_india"}
                        {:when "vip_flag" :group "vip_customer"}]
             :default {:group "standard"}}
            {:kind "logic"
             :name "offer_logic"
             :inputs {"customer_name" "normalize_input.customer_name"
                      "amount_num" "normalize_input.amount_num"
                      "segment" "customer_segment.group"}
             :assignments [{:var "discount" :expr "if(segment = 'priority_india', 0.20, if(segment = 'vip_customer', 0.15, 0.05))"}
                           {:var "score" :expr "if(segment = 'priority_india', amount_num * 2, if(segment = 'vip_customer', amount_num * 1.5, amount_num))"}]
             :outputs {"greeting" "concat('Hello ', customer_name)"
                       "segment" "segment"
                       "discount" "discount"
                       "score" "score"}}]
   :response {:kind "response-builder"
              :template {"greeting" "greeting"
                         "segment" "segment"
                         "discountRate" "discount"
                         "priorityScore" "score"}}
   :assumptions ["Use an endpoint-backed graph for transform generation."
                 "Treat amount as numeric and vip as boolean-like."]})

(defn- invoice-plan [_text]
  {:graph_name "Invoice Review Flow"
   :goal "Normalize invoice inputs, route exceptions, and shape a payment decision"
   :source (endpoint-source "invoice-review" ["invoice_id" "vendor" "amount" "country" "po_number" "urgent"])
   :stages [{:kind "logic"
             :name "normalize_invoice"
             :inputs {"invoice_id_raw" "invoice_id"
                      "vendor_raw" "vendor"
                      "amount_raw" "amount"
                      "country_raw" "country"
                      "po_raw" "po_number"
                      "urgent_raw" "urgent"}
             :assignments [{:var "invoice_id" :expr "trim(invoice_id_raw)"}
                           {:var "vendor_name" :expr "trim(vendor_raw)"}
                           {:var "amount_num" :expr "tonumber(amount_raw)"}
                           {:var "country_norm" :expr "upper(country_raw)"}
                           {:var "po_present" :expr "not(isempty(trim(po_raw)))"}
                           {:var "urgent_flag" :expr "toboolean(urgent_raw)"}]
             :outputs {"invoice_id" "invoice_id"
                       "vendor_name" "vendor_name"
                       "amount_num" "amount_num"
                       "country_norm" "country_norm"
                       "po_present" "po_present"
                       "urgent_flag" "urgent_flag"}}
            {:kind "conditional"
             :name "invoice_route"
             :cond_type "if-elif-else"
             :branches [{:when "amount_num >= 10000" :group "manager_review"}
                        {:when "not(po_present)" :group "po_exception"}
                        {:when "urgent_flag" :group "fast_track"}]
             :default {:group "straight_through"}}
            {:kind "logic"
             :name "payment_projection"
             :inputs {"invoice_id" "normalize_invoice.invoice_id"
                      "vendor_name" "normalize_invoice.vendor_name"
                      "amount_num" "normalize_invoice.amount_num"
                      "route" "invoice_route.group"}
             :assignments [{:var "payment_priority" :expr "if(route = 'fast_track', 'EXPEDITE', if(route = 'manager_review', 'APPROVAL', if(route = 'po_exception', 'HOLD', 'STANDARD')))"}
                           {:var "scheduled_amount" :expr "round(amount_num * if(route = 'manager_review', 0.98, 1.0), 2)"}]
             :outputs {"invoiceId" "invoice_id"
                       "vendor" "vendor_name"
                       "route" "route"
                       "paymentPriority" "payment_priority"
                       "scheduledAmount" "scheduled_amount"}}]
   :response {:kind "response-builder"
              :template {"invoiceId" "invoiceId"
                         "vendor" "vendor"
                         "route" "route"
                         "paymentPriority" "paymentPriority"
                         "scheduledAmount" "scheduledAmount"}}
   :assumptions ["Treat missing PO numbers as policy exceptions."
                 "Use a single decision stage for the first release."]})

(defn- lead-plan [_text]
  {:graph_name "Lead Routing Flow"
   :goal "Score leads and route them to the correct owner"
   :source (endpoint-source "lead-routing" ["name" "title" "region" "employees" "budget"])
   :stages [{:kind "logic"
             :name "score_input"
             :inputs {"lead_name_raw" "name"
                      "title_raw" "title"
                      "region_raw" "region"
                      "employees_raw" "employees"
                      "budget_raw" "budget"}
             :assignments [{:var "lead_name" :expr "trim(lead_name_raw)"}
                           {:var "title_norm" :expr "upper(title_raw)"}
                           {:var "region_norm" :expr "upper(region_raw)"}
                           {:var "employees_num" :expr "tonumber(employees_raw)"}
                           {:var "budget_num" :expr "tonumber(budget_raw)"}]
             :outputs {"lead_name" "lead_name"
                       "title_norm" "title_norm"
                       "region_norm" "region_norm"
                       "employees_num" "employees_num"
                       "budget_num" "budget_num"}}
            {:kind "conditional"
             :name "lead_segment"
             :cond_type "if-elif-else"
             :branches [{:when "budget_num >= 100000 && employees_num >= 1000" :group "exec_priority"}
                        {:when "contains(title_norm, 'CHIEF')" :group "executive_title"}
                        {:when "region_norm = 'NA'" :group "regional_priority"}]
             :default {:group "nurture"}}
            {:kind "logic"
             :name "action_projection"
             :inputs {"lead_name" "score_input.lead_name"
                      "segment" "lead_segment.group"}
             :assignments [{:var "recommended_owner" :expr "if(segment = 'exec_priority', 'Account Executive', if(segment = 'executive_title', 'Enterprise Sales', if(segment = 'regional_priority', 'Regional AE', 'Inside Sales')))"}
                           {:var "next_step" :expr "if(segment = 'nurture', 'Send nurture email', 'Schedule discovery call')"}]
             :outputs {"greeting" "concat('Hello ', lead_name)"
                       "segment" "segment"
                       "recommendedOwner" "recommended_owner"
                       "nextStep" "next_step"}}]
   :response {:kind "response-builder"
              :template {"greeting" "greeting"
                         "segment" "segment"
                         "recommendedOwner" "recommendedOwner"
                         "nextStep" "nextStep"}}
   :assumptions ["Use lead-scoring semantics for executive routing."
                 "Treat employees and budget as numeric inputs."]})

(defn- mastering-plan [_text]
  {:graph_name "Customer Mastering Flow"
   :goal "Standardize customer records, route exceptions, and compute publish decisions"
   :source (endpoint-source "customer-mastering"
                            ["customer_id" "name" "email" "country" "lifetime_value" "status"])
   :stages [{:kind "logic"
             :name "normalize_customer"
             :inputs {"customer_id_raw" "customer_id"
                      "name_raw" "name"
                      "email_raw" "email"
                      "country_raw" "country"
                      "value_raw" "lifetime_value"
                      "status_raw" "status"}
             :assignments [{:var "customer_id" :expr "trim(customer_id_raw)"}
                           {:var "name_norm" :expr "trim(name_raw)"}
                           {:var "email_norm" :expr "lower(trim(email_raw))"}
                           {:var "country_norm" :expr "upper(country_raw)"}
                           {:var "value_num" :expr "tonumber(value_raw)"}
                           {:var "status_norm" :expr "upper(trim(status_raw))"}]
             :outputs {"customer_id" "customer_id"
                       "name_norm" "name_norm"
                       "email_norm" "email_norm"
                       "country_norm" "country_norm"
                       "value_num" "value_num"
                       "status_norm" "status_norm"}}
            {:kind "conditional"
             :name "quality_route"
             :cond_type "if-elif-else"
             :branches [{:when "isempty(email_norm)" :group "steward_review"}
                        {:when "status_norm = 'INACTIVE'" :group "suppress"}
                        {:when "value_num >= 10000" :group "vip_publish"}]
             :default {:group "standard_publish"}}
            {:kind "logic"
             :name "publish_projection"
             :inputs {"customer_id" "normalize_customer.customer_id"
                      "name_norm" "normalize_customer.name_norm"
                      "route" "quality_route.group"}
             :assignments [{:var "publish_flag" :expr "if(route = 'suppress', false, true)"}
                           {:var "target_queue" :expr "if(route = 'steward_review', 'DATA_STEWARD', if(route = 'vip_publish', 'VIP_MASTERING', 'STANDARD_MASTERING'))"}]
             :outputs {"customerId" "customer_id"
                       "displayName" "name_norm"
                       "route" "route"
                       "publishFlag" "publish_flag"
                       "targetQueue" "target_queue"}}]
   :response {:kind "response-builder"
              :template {"customerId" "customerId"
                         "displayName" "displayName"
                         "route" "route"
                         "publishFlag" "publishFlag"
                         "targetQueue" "targetQueue"}}
   :assumptions ["Treat mastering as an endpoint-backed flow for the first draft."
                 "Use simple stewardship routing rules instead of dedup logic."]})

(defn- eligibility-plan [_text]
  {:graph_name "Eligibility Decision Flow"
   :goal "Normalize applicant inputs, route eligibility outcomes, and project an offer decision"
   :source (endpoint-source "eligibility-decision"
                            ["applicant" "country" "income" "credit_score" "existing_customer"])
   :stages [{:kind "logic"
             :name "normalize_application"
             :inputs {"applicant_raw" "applicant"
                      "country_raw" "country"
                      "income_raw" "income"
                      "credit_raw" "credit_score"
                      "existing_raw" "existing_customer"}
             :assignments [{:var "applicant_name" :expr "trim(applicant_raw)"}
                           {:var "country_norm" :expr "upper(country_raw)"}
                           {:var "income_num" :expr "tonumber(income_raw)"}
                           {:var "credit_num" :expr "tonumber(credit_raw)"}
                           {:var "existing_flag" :expr "toboolean(existing_raw)"}]
             :outputs {"applicant_name" "applicant_name"
                       "country_norm" "country_norm"
                       "income_num" "income_num"
                       "credit_num" "credit_num"
                       "existing_flag" "existing_flag"}}
            {:kind "conditional"
             :name "eligibility_route"
             :cond_type "if-elif-else"
             :branches [{:when "credit_num >= 720 && income_num >= 90000" :group "premium_offer"}
                        {:when "existing_flag && credit_num >= 650" :group "loyalty_offer"}
                        {:when "country_norm != 'US'" :group "manual_review"}]
             :default {:group "decline"}}
            {:kind "logic"
             :name "offer_projection"
             :inputs {"applicant_name" "normalize_application.applicant_name"
                      "route" "eligibility_route.group"
                      "income_num" "normalize_application.income_num"}
             :assignments [{:var "apr" :expr "if(route = 'premium_offer', 4.9, if(route = 'loyalty_offer', 6.9, if(route = 'manual_review', 0, 0)))"}
                           {:var "limit_amount" :expr "if(route = 'premium_offer', income_num * 0.4, if(route = 'loyalty_offer', income_num * 0.25, 0))"}]
             :outputs {"applicant" "applicant_name"
                       "decision" "route"
                       "apr" "apr"
                       "limitAmount" "limit_amount"}}]
   :response {:kind "response-builder"
              :template {"applicant" "applicant"
                         "decision" "decision"
                         "apr" "apr"
                         "limitAmount" "limitAmount"}}
   :assumptions ["Use simple eligibility thresholds for the first generated draft."
                 "Assume existing_customer behaves like a boolean flag."]})

(defn- mock-spec [text]
  (case (guess-domain text)
    :invoice (invoice-plan text)
    :lead (lead-plan text)
    :mastering (mastering-plan text)
    :eligibility (eligibility-plan text)
    (order-plan text)))

(defn- normalize-plan [raw]
  (let [m (walk/keywordize-keys (or raw {}))
        source (:source m)
        stages (vec (for [stage (:stages m)]
                      (let [stage' (walk/keywordize-keys stage)]
                        (cond-> {:kind (some-> (:kind stage') str string/lower-case)
                                 :name (:name stage')}
                          (:inputs stage') (assoc :inputs (:inputs stage'))
                          (:assignments stage') (assoc :assignments (:assignments stage'))
                          (:outputs stage') (assoc :outputs (:outputs stage'))
                          (:cond_type stage') (assoc :cond_type (some-> (:cond_type stage') str string/lower-case))
                          (:branches stage') (assoc :branches (:branches stage'))
                          (:default stage') (assoc :default (:default stage'))))))]
    {:graph_name (or (:graph_name m) (:graph-name m))
     :goal (:goal m)
     :source (cond-> {:kind (some-> (:kind source) str string/lower-case)}
               (:http_method source) (assoc :http_method (string/upper-case (str (:http_method source))))
               (:route_path source) (assoc :route_path (:route_path source))
               (:query_params source) (assoc :query_params (vec (:query_params source)))
               (:table_name source) (assoc :table_name (:table_name source)))
     :stages stages
     :response (some-> (:response m) walk/keywordize-keys)
     :assumptions (vec (or (:assumptions m) []))
     :explanations (vec (or (:explanations m) []))}))

(defn- normalize-config-tree
  "Recursively normalize nested config maps to the snake_case keys expected by
   save fns, logic validation, and runtime execution."
  [value]
  (walk/postwalk
   (fn [node]
     (if (map? node)
       (into {} (map (fn [[k v]] [(->config-key k) v]) node))
       node))
   value))

(defn- normalize-transform-gil
  "gil-normalize handles the outer GIL shape, but nested config rows remain in
   kebab-case. Convert them back to the runtime's snake_case contract."
  [gil]
  (update gil :nodes
          (fn [nodes]
            (mapv (fn [node]
                    (update node :config
                            (fn [config]
                              (if (map? config)
                                (normalize-config-tree config)
                                {}))))
                  (or nodes [])))))

(defn- call-transform-llm [system-prompt user-prompt]
  (llm/call-llm system-prompt user-prompt
                :temperature 0
                :tools [transform-tool]
                :tool-choice {:type "tool" :name "create_transform_graph"}))

(defn- translate-with-llm [text]
  (normalize-plan (call-transform-llm (build-system-prompt text) text)))

(defn- plan-shape-valid? [plan]
  (and (non-blank (:graph_name plan))
       (map? (:source plan))
       (contains? allowed-source-kinds (get-in plan [:source :kind]))
       (seq (:stages plan))))

(defn- planner-error-messages [validation]
  (->> (:errors validation)
       (map #(or (:message %) (pr-str %)))
       (remove string/blank?)
       (take 6)
       vec))

(defn- repair-user-prompt [text prior-plan validation]
  (str
   "Original request:\n" text "\n\n"
   "Repair the previous transform tool output so it becomes a valid Bitool transform graph.\n"
   "Return ONLY a corrected create_transform_graph tool call.\n\n"
   "Previous tool output JSON:\n"
   (json/generate-string (or prior-plan {}) {:pretty true}) "\n\n"
   (when validation
     (str "Validation errors to fix:\n"
          (string/join "\n" (map #(str "- " %) (planner-error-messages validation)))
          "\n\n"))
   "Keep the graph linear and preserve the user's business intent."))

(defn- evaluate-plan [plan]
  (cond
    (not (plan-shape-valid? plan))
    {:status :incomplete :plan plan}

    :else
    (try
      (let [gil (semantic-plan->gil plan)
            validation (validate-transform-gil gil)]
        (if (:valid validation)
          {:status :valid
           :plan plan
           :gil gil
           :validation validation}
          {:status :invalid
           :plan plan
           :gil gil
           :validation validation}))
      (catch Exception e
        {:status :invalid
         :plan plan
         :validation {:valid false
                      :errors [{:code :planner_exception
                                :message (.getMessage e)}]}}))))

(defn- attempt-llm-plan [text]
  (try
    (evaluate-plan (translate-with-llm text))
    (catch Exception e
      {:status :llm-error
       :error e})))

(defn- attempt-llm-repair [text prior-plan validation]
  (try
    (let [system-prompt (build-system-prompt text)
          user-prompt (repair-user-prompt text prior-plan validation)
          repaired (normalize-plan (call-transform-llm system-prompt user-prompt))]
      (evaluate-plan repaired))
    (catch Exception e
      {:status :llm-error
       :error e})))

(defn- fallback-plan-result [text reason & [details]]
  (when details
    (log/warn "Transform planner falling back to deterministic spec" (merge {:text text :reason reason} details)))
  {:plan (mock-spec text)
   :planner {:mode :fallback
             :reason reason
             :repair_attempted? (boolean (:repair_attempted? details))}})

(defn- resolve-plan [text]
  (let [initial (attempt-llm-plan text)]
    (cond
      (= :valid (:status initial))
      {:plan (:plan initial)
       :planner {:mode :llm
                 :repair_attempted? false}}

      (#{:incomplete :invalid} (:status initial))
      (loop [remaining max-repair-attempts
             last-attempt initial]
        (if (pos? remaining)
          (let [repaired (attempt-llm-repair text (:plan last-attempt) (:validation last-attempt))]
            (if (= :valid (:status repaired))
              {:plan (:plan repaired)
               :planner {:mode :llm_repair
                         :repair_attempted? true
                         :repair_count (- max-repair-attempts remaining -1)}}
              (recur (dec remaining) repaired)))
          (fallback-plan-result text :invalid_llm_plan
                                {:repair_attempted? true
                                 :errors (planner-error-messages (:validation last-attempt))})))

      :else
      (fallback-plan-result text :llm_unavailable
                            {:error (some-> (:error initial) .getMessage)}))))

(defn parse-text
  "Return a semantic transform plan. Falls back to deterministic templates
   when LLM providers are unavailable or return invalid output."
  [text]
  (:plan (resolve-plan text)))

(defn- unique-aliases [names]
  (let [seen (atom {})]
    (mapv (fn [name]
            (let [base (sanitize-identifier name "stage")
                  n (swap! seen update base (fnil inc 0))
                  idx (get n base)]
              (if (= idx 1) base (str base "_" idx))))
          names)))

(defn semantic-plan->gil [plan]
  (let [graph-name (or (non-blank (:graph_name plan)) "Generated Transform Graph")
        source-kind (or (some-> plan :source :kind non-blank) "endpoint")
        source-node (if (= source-kind "table")
                      {:node-ref "src"
                       :type "table"
                       :alias (sanitize-identifier (or (get-in plan [:source :table_name]) "source_table") "source_table")
                       :config {}}
                      {:node-ref "src"
                       :type "endpoint"
                       :alias "request_input"
                       :config {:http_method (or (get-in plan [:source :http_method]) "GET")
                                :route_path (or (get-in plan [:source :route_path]) (str "/ai/" (slugify graph-name)))
                                :query_params (mapv (fn [param]
                                                      {:param_name param :data_type "string"})
                                                    (or (get-in plan [:source :query_params]) []))
                                :response_format "json"
                                :description (or (:goal plan) graph-name)}})
        stages (:stages plan)
        aliases (unique-aliases (map :name stages))
        stage-nodes
        (mapv (fn [idx stage alias]
                (let [kind (:kind stage)
                      node-ref (str "n" (inc idx))]
                  (if (= kind "logic")
                    {:node-ref node-ref
                     :type "function"
                     :alias alias
                     :config {:fn_name alias
                              :fn_params (mapv (fn [[param source]]
                                                 {:param_name (sanitize-identifier param "param")
                                                  :source_column (str source)})
                                               (or (:inputs stage) {}))
                              :fn_lets (mapv (fn [{:keys [var expr]}]
                                               {:variable (sanitize-identifier var "tmp")
                                                :expression (str expr)})
                                             (or (:assignments stage) []))
                              :fn_outputs (mapv (fn [[output-name expr]]
                                                 {:output_name (sanitize-identifier output-name "result")
                                                  :data_type "varchar"
                                                  :expression (str expr)})
                                               (or (:outputs stage) {}))}}
                    {:node-ref node-ref
                     :type "conditionals"
                     :alias alias
                     :config {:cond_type (or (:cond_type stage) "if-else")
                              :branches (mapv (fn [branch]
                                                (cond-> {:group (str (:group branch))}
                                                  (:when branch) (assoc :condition (str (:when branch)))
                                                  (:guard branch) (assoc :guard (str (:guard branch)))
                                                  (:value branch) (assoc :value (str (:value branch)))))
                                              (or (:branches stage) []))
                              :default_branch (or (get-in stage [:default :group]) "")}})))
              (range)
              stages
              aliases)
        response-node (when (= source-kind "endpoint")
                        {:node-ref "rb"
                         :type "response-builder"
                         :alias "response_builder"
                         :config {:status_code "200"
                                  :response_type "json"
                                  :headers ""
                                  :template (mapv (fn [[k v]]
                                                    {:output_key (str k)
                                                     :source_column (str v)})
                                                  (or (get-in plan [:response :template]) {}))}})
        output-node {:node-ref "out" :type "output" :alias "Output"}
        nodes (vec (concat [source-node] stage-nodes (when response-node [response-node]) [output-node]))
        ordered-refs (mapv :node-ref nodes)
        edges (->> ordered-refs
                   (partition 2 1)
                   (mapv vec))]
    {:gil-version "1.0"
     :intent :build
     :graph-name graph-name
     :description (or (:goal plan) graph-name)
     :nodes nodes
     :edges edges
     :assumptions (vec (or (:assumptions plan) []))
     :explanations (vec (or (:explanations plan) []))}))

(defn- source-fields [node]
  (case (:type node)
    "endpoint" (set (keep (comp non-blank :param_name) (get-in node [:config :query_params])))
    #{}))

(defn- function-output-fields [node]
  (let [outs (or (get-in node [:config :fn_outputs]) [])]
    (if (seq outs)
      (set (keep (comp non-blank :output_name) outs))
      (if-let [expr (non-blank (get-in node [:config :fn_return]))]
        #{"result"}
        #{}))))

(defn- stage-exports [node]
  (case (:type node)
    "function" (function-output-fields node)
    "conditionals" allowed-conditional-outputs
    #{}))

(defn- dot-ref->parts [value]
  (when (semantic-ref? value)
    (let [[stage field] (string/split value #"\." 2)]
      [stage field])))

(defn- error [code message & [extra]]
  (merge {:code code :message message} (or extra {})))

(defn- linear-topology-errors [nodes edges]
  (let [incoming (reduce (fn [m [_ to]] (update m to (fnil inc 0))) {} edges)
        outgoing (reduce (fn [m [from _]] (update m from (fnil inc 0))) {} edges)
        refs (mapv :node-ref nodes)
        by-ref (into {} (map (juxt :node-ref identity) nodes))
        source-refs (filter #(contains? #{"endpoint" "table"} (:type (get by-ref %))) refs)
        output-refs (filter #(= "output" (:type (get by-ref %))) refs)
        response-count (count (filter #(= "response-builder" (:type %)) nodes))
        errors (transient [])]
    (when-not (= 1 (count source-refs))
      (conj! errors (error :invalid-source-count "Transform graphs must contain exactly one source node.")))
    (when-not (= 1 (count output-refs))
      (conj! errors (error :invalid-output-count "Transform graphs must contain exactly one output node.")))
    (when (> response-count 1)
      (conj! errors (error :invalid-response-builder-count "Transform graphs may contain at most one response-builder node.")))
    (when-not (= (count edges) (dec (count refs)))
      (conj! errors (error :non_linear_edges "Transform graphs must be a single linear chain.")))
    (doseq [ref refs]
      (let [node (get by-ref ref)
            in (get incoming ref 0)
            out (get outgoing ref 0)]
        (case (:type node)
          ("endpoint" "table")
          (when-not (= 1 out)
            (conj! errors (error :invalid-source-outgoing "Source node must have exactly one outgoing edge." {:ref ref})))
          "output"
          (do
            (when-not (= 1 in)
              (conj! errors (error :invalid-output-incoming "Output node must have exactly one incoming edge." {:ref ref})))
            (when-not (zero? out)
              (conj! errors (error :invalid-output-outgoing "Output node must have no outgoing edges." {:ref ref}))))
          (do
            (when-not (= 1 in)
              (conj! errors (error :invalid-node-incoming "Transform stage must have exactly one incoming edge." {:ref ref :type (:type node)})))
            (when-not (= 1 out)
              (conj! errors (error :invalid-node-outgoing "Transform stage must have exactly one outgoing edge." {:ref ref :type (:type node)})))))))
    (persistent! errors)))

(defn validate-transform-gil
  "Custom validation for the constrained transform GIL subset.
   Returns {:valid ... :errors [...] :warnings [...] :normalized-gil gil}."
  [gil]
  (let [norm-gil (-> gil
                     gil-normalize/normalize
                     normalize-transform-gil)
        nodes (or (:nodes norm-gil) [])
        edges (or (:edges norm-gil) [])
        errs (atom [])
        warns (atom [])]
    (doseq [node nodes]
      (when-not (contains? allowed-node-types (:type node))
        (swap! errs conj (error :disallowed-node-type
                                (str "Node type '" (:type node) "' is outside the v1 transform scope.")
                                {:ref (:node-ref node) :type (:type node)}))))
    (doseq [e (linear-topology-errors nodes edges)]
      (swap! errs conj e))

    (let [ordered (mapv second (sort-by first (map-indexed vector nodes)))
          alias-map (atom {})
          accessible-raw (atom #{})]
      (doseq [node ordered]
        (let [alias (sanitize-identifier (:alias node) "stage")
              exports (stage-exports node)]
          (when (= (:type node) "endpoint")
            (swap! accessible-raw set/union (source-fields node)))
          (when (= (:type node) "function")
            (try
              (logic/validate-logic-config! (get node :config))
              (catch Exception e
                (swap! errs conj (error :invalid-function-config (.getMessage e) {:ref (:node-ref node)}))))
            (doseq [param (or (get-in node [:config :fn_params]) [])]
              (when-let [source (non-blank (:source_column param))]
                (if-let [[stage field] (dot-ref->parts source)]
                  (let [seen-exports (get @alias-map stage)]
                    (cond
                      (nil? seen-exports)
                      (swap! errs conj (error :unknown-stage-ref
                                              (str "Stage reference '" source "' refers to an unknown or later stage.")
                                              {:ref (:node-ref node) :source_column source}))
                      (not (contains? seen-exports field))
                      (swap! errs conj (error :unknown-stage-output
                                              (str "Stage reference '" source "' is invalid.")
                                              {:ref (:node-ref node)
                                               :source_column source
                                               :allowed (vec (sort seen-exports))}))))
                  (when (and (seq @accessible-raw)
                             (not (contains? @accessible-raw source)))
                    (swap! warns conj (error :unknown-flat-source
                                             (str "Source column '" source "' is not a known upstream field.")
                                             {:ref (:node-ref node) :source_column source})))))))
          (when (= (:type node) "conditionals")
            (try
              (logic/validate-conditional-config! (assoc (get node :config) :id (:node-ref node)))
              (catch Exception e
                (swap! errs conj (error :invalid-conditional-config (.getMessage e) {:ref (:node-ref node)})))))
          (when (= (:type node) "response-builder")
            (doseq [row (or (get-in node [:config :template]) [])]
              (when-let [source (non-blank (:source_column row))]
                (if-let [[stage field] (dot-ref->parts source)]
                  (let [seen-exports (get @alias-map stage)]
                    (cond
                      (nil? seen-exports)
                      (swap! errs conj (error :unknown-stage-ref
                                              (str "Response template reference '" source "' refers to an unknown or later stage.")
                                              {:ref (:node-ref node) :source_column source}))
                      (not (contains? seen-exports field))
                      (swap! errs conj (error :unknown-stage-output
                                              (str "Response template reference '" source "' is invalid.")
                                              {:ref (:node-ref node)
                                               :source_column source
                                               :allowed (vec (sort seen-exports))}))))
                  (when (and (seq @accessible-raw)
                             (not (contains? @accessible-raw source)))
                    (swap! warns conj (error :unknown-response-source
                                             (str "Response template source '" source "' is not a known upstream field.")
                                             {:ref (:node-ref node) :source_column source})))))))
          (swap! alias-map assoc alias exports)
          (swap! accessible-raw set/union exports))))

    (let [gil-validation (gil-validator/validate norm-gil)
          errors (vec (concat @errs (:errors gil-validation)))
          warnings (vec (concat @warns (:warnings gil-validation)))]
      {:valid (empty? errors)
       :errors errors
       :warnings warnings
       :normalized-gil norm-gil})))

(defn preview-data [plan gil validation]
  (let [nodes (:nodes gil)
        source (first nodes)
        stage-nodes (filter #(contains? #{"function" "conditionals"} (:type %)) nodes)
        response-node (first (filter #(= "response-builder" (:type %)) nodes))
        source-label (case (:type source)
                       "endpoint" (str "Endpoint " (get-in source [:config :route_path]))
                       "table" (str "Table " (or (:alias source) "source"))
                       (:type source))]
    {:graph_name (:graph-name gil)
     :goal (or (:goal plan) (:description gil))
     :shape (string/join " -> " (map (fn [node]
                                       (case (:type node)
                                         "endpoint" "Endpoint"
                                         "table" "Table"
                                         "function" (str "Logic(" (:alias node) ")")
                                         "conditionals" (str "Conditional(" (:alias node) ")")
                                         "response-builder" "Response Builder"
                                         "output" "Output"
                                         (:type node)))
                                     nodes))
     :source source-label
     :stage_count (count stage-nodes)
     :response_fields (vec (map :output_key (or (get-in response-node [:config :template]) [])))
     :assumptions (vec (or (:assumptions plan) []))
     :warnings (:warnings validation)}))

(defn preview-text [preview]
  (str
   "Graph: " (:graph_name preview) "\n"
   "Goal: " (:goal preview) "\n"
   "Source: " (:source preview) "\n"
   "Flow: " (:shape preview)
   (when (seq (:response_fields preview))
     (str "\nResponse: " (string/join ", " (:response_fields preview))))
   (when (seq (:assumptions preview))
     (str "\nAssumptions:\n"
          (string/join "\n" (map #(str "- " %) (:assumptions preview)))))))

(defn plan-from-text
  "Translate text into a constrained transform graph plan."
  [text]
  (let [{:keys [plan planner]} (resolve-plan text)
        gil (semantic-plan->gil plan)
        validation (validate-transform-gil gil)
        preview (preview-data plan gil validation)]
    {:plan plan
     :planner planner
     :gil gil
     :validation validation
     :preview preview
     :text (preview-text preview)}))
