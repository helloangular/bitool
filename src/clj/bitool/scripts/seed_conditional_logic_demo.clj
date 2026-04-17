(ns bitool.scripts.seed-conditional-logic-demo
  (:require [bitool.db :as db]
            [bitool.endpoint :as endpoint]
            [bitool.graph2 :as g2]
            [cheshire.core :as json]))

(defn- empty-graph
  [graph-name]
  {:a {:name graph-name :v 0 :id 0}
   :n {1 {:na {:name "O" :btype "O" :tcols {}}
          :e  {}}}})

(defn- base-demo-graph
  [graph-name endpoint-name route-path]
  (-> (empty-graph graph-name)
      (g2/add-node {:name endpoint-name :btype "Ep" :tcols {}})
      (g2/add-node {:name "customer_segment" :btype "C" :tcols {}})
      (g2/add-node {:name "offer_logic" :btype "Fu" :tcols {}})
      (g2/add-node {:name "response_builder" :btype "Rb" :tcols {}})
      (g2/add-edges [[2 3] [3 4] [4 5] [5 1]])
      (g2/save-endpoint
       2
       {:http_method "GET"
        :route_path route-path
        :response_format "json"
        :description "Demo endpoint that feeds a conditional node and a logic node together."
        :query_params [{:param_name "name" :data_type "varchar" :required true}
                       {:param_name "country" :data_type "varchar" :required false}
                       {:param_name "amount" :data_type "double" :required false}
                       {:param_name "vip" :data_type "boolean" :required false}
                       {:param_name "status" :data_type "varchar" :required false}
                       {:param_name "channel" :data_type "varchar" :required false}
                       {:param_name "plan" :data_type "varchar" :required false}
                       {:param_name "region" :data_type "varchar" :required false}]
        :path_params []
        :body_schema []})))

(defn- save-logic-and-response
  [g conditional-node-id logic-params]
  (-> g
      (g2/save-logic 4 logic-params)
      (g2/save-response-builder
       5
       {:status_code "200"
        :response_type "json"
        :headers ""
        :template [{:output_key "name" :source_column "name"}
                   {:output_key "status" :source_column "status"}
                   {:output_key "channel" :source_column "channel"}
                   {:output_key "plan" :source_column "plan"}
                   {:output_key "country" :source_column "country"}
                   {:output_key "amount" :source_column "amount"}
                   {:output_key "segment" :source_column (str "cond_" conditional-node-id "_group")}
                   {:output_key "matchedCondition"
                    :source_column (str "cond_" conditional-node-id "_condition")}
                   {:output_key "usedDefault"
                    :source_column (str "cond_" conditional-node-id "_used_default")}
                   {:output_key "greeting" :source_column "greeting"}
                   {:output_key "offerLabel" :source_column "offer_label"}
                   {:output_key "priorityScore" :source_column "priority_score"}]})))

(defn- insert-demo!
  [graph]
  (db/insertGraph graph))

(defn- sample-run
  [graph-id params]
  (-> (endpoint/execute-graph
       graph-id
       2
       {:path {} :query params :body {}}
       {:headers {} :remote-addr "127.0.0.1"})
      :body
      (json/parse-string)))

(defn- create-if-elif-demo!
  []
  (let [graph
        (-> (base-demo-graph "Conditional + Logic Demo" "demo_request" "/demo/conditional-logic")
            (g2/save-conditional
             3
             {:cond_type "if-elif-else"
              :branches [{:condition "upper(country) = 'INDIA' && tonumber(amount) >= 1000"
                          :group "priority_india"}
                         {:condition "toboolean(vip)"
                          :group "vip_customer"}]
              :default_branch "standard"})
            (save-logic-and-response
             3
             {:fn_name "OfferLogic"
              :fn_params [{:param_name "customer_name" :source_column "name"}
                          {:param_name "spend" :source_column "amount"}
                          {:param_name "segment" :source_column "cond_3_group"}]
              :fn_lets [{:variable "normalized_name" :expression "trim(customer_name)"}
                        {:variable "spend_value" :expression "tonumber(spend)"}
                        {:variable "offer_label_calc"
                         :expression "if(segment = 'priority_india', 'Priority India Offer', if(segment = 'vip_customer', 'VIP Offer', 'Standard Offer'))"}
                        {:variable "priority_score_calc"
                         :expression "if(segment = 'priority_india', spend_value * 2, if(segment = 'vip_customer', spend_value * 1.5, spend_value))"}]
              :fn_return ""
              :fn_outputs [{:output_name "greeting"
                            :data_type "varchar"
                            :expression "concat('Hello ', normalized_name)"}
                           {:output_name "offer_label"
                            :data_type "varchar"
                            :expression "offer_label_calc"}
                           {:output_name "priority_score"
                            :data_type "double"
                            :expression "priority_score_calc"}]}))
        saved (insert-demo! graph)
        gid   (get-in saved [:a :id])]
    {:name "Conditional + Logic Demo"
     :id gid
     :sample-request {:name "Asha" :country "india" :amount "1250" :vip "false"}
     :sample-response (sample-run gid {:name "Asha" :country "india" :amount "1250" :vip "false"})}))

(defn- create-case-demo!
  []
  (let [graph
        (-> (base-demo-graph "Case + Logic Demo" "case_request" "/demo/case-logic")
            (g2/save-conditional
             3
             {:cond_type "case"
              :branches [{:condition "upper(status)" :value "'OPEN'" :group "active_work"}
                         {:condition "upper(status)" :value "'PENDING'" :group "needs_followup"}
                         {:condition "upper(status)" :value "'CLOSED'" :group "completed"}]
              :default_branch "unknown_status"})
            (save-logic-and-response
             3
             {:fn_name "CaseOfferLogic"
              :fn_params [{:param_name "customer_name" :source_column "name"}
                          {:param_name "segment" :source_column "cond_3_group"}
                          {:param_name "workflow_status" :source_column "status"}]
              :fn_lets [{:variable "normalized_name" :expression "trim(customer_name)"}
                        {:variable "offer_label_calc"
                         :expression "if(segment = 'active_work', 'Open Case Rescue', if(segment = 'needs_followup', 'Pending Reminder', if(segment = 'completed', 'Closed Winback', 'Status Review'))) "}
                        {:variable "priority_score_calc"
                         :expression "if(segment = 'active_work', 90, if(segment = 'needs_followup', 70, if(segment = 'completed', 20, 10)))"}]
              :fn_return ""
              :fn_outputs [{:output_name "greeting"
                            :data_type "varchar"
                            :expression "concat('Case for ', normalized_name, ': ', workflow_status)"}
                           {:output_name "offer_label"
                            :data_type "varchar"
                            :expression "offer_label_calc"}
                           {:output_name "priority_score"
                            :data_type "double"
                            :expression "priority_score_calc"}]}))
        saved (insert-demo! graph)
        gid   (get-in saved [:a :id])]
    {:name "Case + Logic Demo"
     :id gid
     :sample-request {:name "Morgan" :status "PENDING"}
     :sample-response (sample-run gid {:name "Morgan" :status "PENDING"})}))

(defn- create-pattern-demo!
  []
  (let [graph
        (-> (base-demo-graph "Pattern Match + Logic Demo" "pattern_request" "/demo/pattern-match-logic")
            (g2/save-conditional
             3
             {:cond_type "pattern-match"
              :headers ["channel" "plan" "region"]
              :branches [{:guard "upper(channel) = 'WEB' && upper(plan) = 'ENTERPRISE'"
                          :group "enterprise_web"}
                         {:guard "upper(channel) = 'PARTNER' && upper(region) = 'EMEA'"
                          :group "partner_emea"}
                         {:guard "upper(channel) = 'MOBILE'"
                          :group "mobile_first"}]
              :default_branch "general"})
            (save-logic-and-response
             3
             {:fn_name "PatternOfferLogic"
              :fn_params [{:param_name "customer_name" :source_column "name"}
                          {:param_name "segment" :source_column "cond_3_group"}
                          {:param_name "channel_value" :source_column "channel"}]
              :fn_lets [{:variable "normalized_name" :expression "trim(customer_name)"}
                        {:variable "offer_label_calc"
                         :expression "if(segment = 'enterprise_web', 'Enterprise Concierge', if(segment = 'partner_emea', 'Partner EMEA Boost', if(segment = 'mobile_first', 'Mobile Acceleration', 'General Journey'))) "}
                        {:variable "priority_score_calc"
                         :expression "if(segment = 'enterprise_web', 120, if(segment = 'partner_emea', 95, if(segment = 'mobile_first', 80, 40)))"}]
              :fn_return ""
              :fn_outputs [{:output_name "greeting"
                            :data_type "varchar"
                            :expression "concat('Hi ', normalized_name, ' via ', channel_value)"}
                           {:output_name "offer_label"
                            :data_type "varchar"
                            :expression "offer_label_calc"}
                           {:output_name "priority_score"
                            :data_type "double"
                            :expression "priority_score_calc"}]}))
        saved (insert-demo! graph)
        gid   (get-in saved [:a :id])]
    {:name "Pattern Match + Logic Demo"
     :id gid
     :sample-request {:name "Riya" :channel "WEB" :plan "ENTERPRISE" :region "APAC"}
     :sample-response (sample-run gid {:name "Riya" :channel "WEB" :plan "ENTERPRISE" :region "APAC"})}))

(defn- create-plsql-style-transform-demo!
  []
  (let [graph
        (-> (empty-graph "PLSQL-Style Transform Demo")
            (g2/add-node {:name "order_input" :btype "Ep" :tcols {}})
            (g2/add-node {:name "normalize_order" :btype "Fu" :tcols {}})
            (g2/add-node {:name "review_route" :btype "C" :tcols {}})
            (g2/add-node {:name "pricing_engine" :btype "Fu" :tcols {}})
            (g2/add-node {:name "release_gate" :btype "C" :tcols {}})
            (g2/add-node {:name "final_projection" :btype "Fu" :tcols {}})
            (g2/add-node {:name "response_builder" :btype "Rb" :tcols {}})
            (g2/add-edges [[2 3] [3 4] [4 5] [5 6] [6 7] [7 8] [8 1]])
            (g2/save-endpoint
             2
             {:http_method "GET"
              :route_path "/demo/plsql-transform"
              :response_format "json"
              :description "A procedural, multi-step transform demo showing how logic and conditional nodes can replace Python-style transform scripting."
              :query_params [{:param_name "order_id" :data_type "varchar" :required true}
                             {:param_name "customer_name" :data_type "varchar" :required true}
                             {:param_name "state" :data_type "varchar" :required true}
                             {:param_name "channel" :data_type "varchar" :required true}
                             {:param_name "tier" :data_type "varchar" :required true}
                             {:param_name "order_amount" :data_type "double" :required true}
                             {:param_name "item_count" :data_type "integer" :required true}
                             {:param_name "rush" :data_type "boolean" :required false}
                             {:param_name "coupon_code" :data_type "varchar" :required false}]
              :path_params []
              :body_schema []})
            (g2/save-logic
             3
             {:fn_name "NormalizeOrderInput"
              :fn_params [{:param_name "raw_order_id" :source_column "order_id"}
                          {:param_name "raw_customer_name" :source_column "customer_name"}
                          {:param_name "raw_state" :source_column "state"}
                          {:param_name "raw_channel" :source_column "channel"}
                          {:param_name "raw_tier" :source_column "tier"}
                          {:param_name "raw_amount" :source_column "order_amount"}
                          {:param_name "raw_item_count" :source_column "item_count"}
                          {:param_name "raw_rush" :source_column "rush"}
                          {:param_name "raw_coupon" :source_column "coupon_code"}]
              :fn_lets [{:variable "order_key_calc" :expression "upper(trim(raw_order_id))"}
                        {:variable "customer_name_calc" :expression "trim(raw_customer_name)"}
                        {:variable "state_norm_calc" :expression "upper(trim(raw_state))"}
                        {:variable "channel_norm_calc" :expression "upper(trim(raw_channel))"}
                        {:variable "tier_norm_calc" :expression "upper(trim(raw_tier))"}
                        {:variable "amount_num_calc" :expression "round(tonumber(raw_amount), 2)"}
                        {:variable "item_count_calc" :expression "tonumber(raw_item_count)"}
                        {:variable "rush_flag_calc" :expression "toboolean(raw_rush)"}
                        {:variable "coupon_trim_calc" :expression "trim(coalesce(raw_coupon, ''))"}
                        {:variable "coupon_present_calc" :expression "not(isempty(coupon_trim_calc))"}
                        {:variable "base_discount_calc" :expression "if(tier_norm_calc = 'PLATINUM', 0.12, if(tier_norm_calc = 'GOLD', 0.08, if(tier_norm_calc = 'SILVER', 0.04, 0.0)))"}
                        {:variable "bulk_discount_calc" :expression "if(item_count_calc >= 10, 0.05, if(item_count_calc >= 5, 0.02, 0.0))"}
                        {:variable "channel_fee_calc" :expression "if(channel_norm_calc = 'MARKETPLACE', 12.0, if(channel_norm_calc = 'STORE', 0.0, 5.0))"}]
              :fn_return ""
              :fn_outputs [{:output_name "order_key" :data_type "varchar" :expression "order_key_calc"}
                           {:output_name "customer_name_clean" :data_type "varchar" :expression "customer_name_calc"}
                           {:output_name "state_norm" :data_type "varchar" :expression "state_norm_calc"}
                           {:output_name "channel_norm" :data_type "varchar" :expression "channel_norm_calc"}
                           {:output_name "tier_norm" :data_type "varchar" :expression "tier_norm_calc"}
                           {:output_name "amount_num" :data_type "double" :expression "amount_num_calc"}
                           {:output_name "item_count_num" :data_type "double" :expression "item_count_calc"}
                           {:output_name "rush_flag" :data_type "boolean" :expression "rush_flag_calc"}
                           {:output_name "coupon_code_clean" :data_type "varchar" :expression "coupon_trim_calc"}
                           {:output_name "coupon_present" :data_type "boolean" :expression "coupon_present_calc"}
                           {:output_name "base_discount" :data_type "double" :expression "base_discount_calc"}
                           {:output_name "bulk_discount" :data_type "double" :expression "bulk_discount_calc"}
                           {:output_name "channel_fee" :data_type "double" :expression "channel_fee_calc"}]})
            (g2/save-conditional
             4
             {:cond_type "if-elif-else"
              :branches [{:condition "amount_num >= 5000 && rush_flag" :group "manual_review"}
                         {:condition "state_norm = 'AK' || state_norm = 'HI'" :group "special_shipping"}
                         {:condition "channel_norm = 'MARKETPLACE' && coupon_present" :group "promo_audit"}
                         {:condition "tier_norm = 'PLATINUM' && amount_num >= 2500" :group "vip_fast_track"}]
              :default_branch "straight_through"})
            (g2/save-logic
             5
             {:fn_name "PricingEngine"
              :fn_params [{:param_name "route_group" :source_column "cond_4_group"}
                          {:param_name "amount_value" :source_column "amount_num"}
                          {:param_name "item_count_value" :source_column "item_count_num"}
                          {:param_name "channel_fee_value" :source_column "channel_fee"}
                          {:param_name "base_discount_value" :source_column "base_discount"}
                          {:param_name "bulk_discount_value" :source_column "bulk_discount"}
                          {:param_name "coupon_present_value" :source_column "coupon_present"}
                          {:param_name "state_value" :source_column "state_norm"}
                          {:param_name "channel_value" :source_column "channel_norm"}
                          {:param_name "rush_value" :source_column "rush_flag"}]
              :fn_lets [{:variable "coupon_discount_calc" :expression "if(coupon_present_value, 0.03, 0.0)"}
                        {:variable "raw_discount_rate_calc" :expression "base_discount_value + bulk_discount_value + coupon_discount_calc"}
                        {:variable "discount_rate_calc" :expression "min(0.20, raw_discount_rate_calc)"}
                        {:variable "discount_amount_calc" :expression "round(amount_value * discount_rate_calc, 2)"}
                        {:variable "net_amount_calc" :expression "round(amount_value - discount_amount_calc + channel_fee_value, 2)"}
                        {:variable "shipping_priority_calc" :expression "if(route_group = 'manual_review', 'HOLD', if(route_group = 'special_shipping', 'EXPEDITE', if(rush_value, 'RUSH', 'STANDARD')))"}
                        {:variable "warehouse_calc" :expression "if(state_value = 'CA' || state_value = 'WA' || state_value = 'OR', 'WEST_DC', if(state_value = 'NY' || state_value = 'NJ' || state_value = 'MA', 'EAST_DC', 'CENTRAL_DC'))"}
                        {:variable "transform_score_calc" :expression "if(route_group = 'manual_review', 95, if(route_group = 'promo_audit', 72, if(route_group = 'vip_fast_track', 40, if(rush_value, 55, 25))))"}
                        {:variable "approval_note_calc" :expression "concat(route_group, ' | ', channel_value, ' | items=', tostring(item_count_value))"}]
              :fn_return ""
              :fn_outputs [{:output_name "discount_rate" :data_type "double" :expression "discount_rate_calc"}
                           {:output_name "discount_amount" :data_type "double" :expression "discount_amount_calc"}
                           {:output_name "net_amount" :data_type "double" :expression "net_amount_calc"}
                           {:output_name "shipping_priority" :data_type "varchar" :expression "shipping_priority_calc"}
                           {:output_name "warehouse_code" :data_type "varchar" :expression "warehouse_calc"}
                           {:output_name "transform_score" :data_type "double" :expression "transform_score_calc"}
                           {:output_name "approval_note" :data_type "varchar" :expression "approval_note_calc"}]})
            (g2/save-conditional
             6
             {:cond_type "cond"
              :branches [{:condition "net_amount >= 3000 && cond_4_group = 'manual_review'" :group "finance_hold"}
                         {:condition "shipping_priority = 'EXPEDITE' && warehouse_code = 'CENTRAL_DC'" :group "carrier_override"}
                         {:condition "discount_rate >= 0.15" :group "manager_signoff"}
                         {:condition "cond_4_group = 'vip_fast_track'" :group "vip_release"}]
              :default_branch "auto_release"})
            (g2/save-logic
             7
             {:fn_name "FinalProjection"
              :fn_params [{:param_name "order_key_value" :source_column "order_key"}
                          {:param_name "customer_name_value" :source_column "customer_name_clean"}
                          {:param_name "route_group_value" :source_column "cond_4_group"}
                          {:param_name "release_group_value" :source_column "cond_6_group"}
                          {:param_name "warehouse_value" :source_column "warehouse_code"}
                          {:param_name "net_amount_value" :source_column "net_amount"}
                          {:param_name "discount_rate_value" :source_column "discount_rate"}
                          {:param_name "transform_score_value" :source_column "transform_score"}
                          {:param_name "shipping_priority_value" :source_column "shipping_priority"}]
              :fn_lets [{:variable "execution_path_calc" :expression "concat(route_group_value, ' -> ', release_group_value)"}
                        {:variable "final_action_calc" :expression "if(release_group_value = 'finance_hold', 'Queue for finance approval', if(release_group_value = 'carrier_override', 'Route to special carrier desk', if(release_group_value = 'manager_signoff', 'Send to sales manager', if(release_group_value = 'vip_release', 'Fast-track VIP release', 'Auto release to warehouse'))))"}
                        {:variable "revenue_band_calc" :expression "if(net_amount_value >= 4000, 'ENTERPRISE', if(net_amount_value >= 1500, 'MID_MARKET', 'SMB'))"}
                        {:variable "ops_queue_calc" :expression "concat(warehouse_value, ':', shipping_priority_value)"}
                        {:variable "audit_summary_calc" :expression "concat(order_key_value, ' | ', customer_name_value, ' | discount=', tostring(round(discount_rate_value * 100, 2)), '% | score=', tostring(transform_score_value))"}]
              :fn_return ""
              :fn_outputs [{:output_name "execution_path" :data_type "varchar" :expression "execution_path_calc"}
                           {:output_name "final_action" :data_type "varchar" :expression "final_action_calc"}
                           {:output_name "revenue_band" :data_type "varchar" :expression "revenue_band_calc"}
                           {:output_name "ops_queue" :data_type "varchar" :expression "ops_queue_calc"}
                           {:output_name "audit_summary" :data_type "varchar" :expression "audit_summary_calc"}]})
            (g2/save-response-builder
             8
             {:status_code "200"
              :response_type "json"
              :headers ""
              :template [{:output_key "orderId" :source_column "order_id"}
                         {:output_key "orderKey" :source_column "order_key"}
                         {:output_key "customerName" :source_column "customer_name_clean"}
                         {:output_key "state" :source_column "state_norm"}
                         {:output_key "channel" :source_column "channel_norm"}
                         {:output_key "tier" :source_column "tier_norm"}
                         {:output_key "amount" :source_column "amount_num"}
                         {:output_key "discountRate" :source_column "discount_rate"}
                         {:output_key "discountAmount" :source_column "discount_amount"}
                         {:output_key "netAmount" :source_column "net_amount"}
                         {:output_key "reviewRoute" :source_column "cond_4_group"}
                         {:output_key "releaseDecision" :source_column "cond_6_group"}
                         {:output_key "shippingPriority" :source_column "shipping_priority"}
                         {:output_key "warehouseCode" :source_column "warehouse_code"}
                         {:output_key "executionPath" :source_column "execution_path"}
                         {:output_key "finalAction" :source_column "final_action"}
                         {:output_key "revenueBand" :source_column "revenue_band"}
                         {:output_key "opsQueue" :source_column "ops_queue"}
                         {:output_key "auditSummary" :source_column "audit_summary"}
                         {:output_key "transformScore" :source_column "transform_score"}
                         {:output_key "reviewCondition" :source_column "cond_4_condition"}
                         {:output_key "releaseCondition" :source_column "cond_6_condition"}]}))
        saved (insert-demo! graph)
        gid   (get-in saved [:a :id])]
    {:name "PLSQL-Style Transform Demo"
     :id gid
     :sample-request {:order_id "SO-1049"
                      :customer_name "Acme Retail West"
                      :state "CA"
                      :channel "marketplace"
                      :tier "gold"
                      :order_amount "6425.50"
                      :item_count "12"
                      :rush "true"
                      :coupon_code "FLASH30"}
     :sample-response (sample-run gid {:order_id "SO-1049"
                                       :customer_name "Acme Retail West"
                                       :state "CA"
                                       :channel "marketplace"
                                       :tier "gold"
                                       :order_amount "6425.50"
                                       :item_count "12"
                                       :rush "true"
                                       :coupon_code "FLASH30"})}))

(defn- create-invoice-eligibility-demo!
  []
  (let [graph
        (-> (empty-graph "Invoice Eligibility + Cleansing Demo")
            (g2/add-node {:name "invoice_input" :btype "Ep" :tcols {}})
            (g2/add-node {:name "cleanse_invoice" :btype "Fu" :tcols {}})
            (g2/add-node {:name "validation_route" :btype "C" :tcols {}})
            (g2/add-node {:name "payment_policy" :btype "Fu" :tcols {}})
            (g2/add-node {:name "release_decision" :btype "C" :tcols {}})
            (g2/add-node {:name "invoice_projection" :btype "Fu" :tcols {}})
            (g2/add-node {:name "response_builder" :btype "Rb" :tcols {}})
            (g2/add-edges [[2 3] [3 4] [4 5] [5 6] [6 7] [7 8] [8 1]])
            (g2/save-endpoint
             2
             {:http_method "GET"
              :route_path "/demo/invoice-eligibility"
              :response_format "json"
              :description "An invoice cleansing and approval-routing demo that replaces Python-style ETL scripting with logic and conditional nodes."
              :query_params [{:param_name "invoice_id" :data_type "varchar" :required true}
                             {:param_name "supplier_name" :data_type "varchar" :required true}
                             {:param_name "supplier_country" :data_type "varchar" :required true}
                             {:param_name "business_unit" :data_type "varchar" :required true}
                             {:param_name "invoice_amount" :data_type "double" :required true}
                             {:param_name "tax_amount" :data_type "double" :required true}
                             {:param_name "currency" :data_type "varchar" :required true}
                             {:param_name "payment_terms" :data_type "varchar" :required true}
                             {:param_name "po_number" :data_type "varchar" :required false}
                             {:param_name "due_days" :data_type "integer" :required true}
                             {:param_name "has_attachment" :data_type "boolean" :required false}
                             {:param_name "cost_center" :data_type "varchar" :required false}]
              :path_params []
              :body_schema []})
            (g2/save-logic
             3
             {:fn_name "CleanseInvoice"
              :fn_params [{:param_name "raw_invoice_id" :source_column "invoice_id"}
                          {:param_name "raw_supplier_name" :source_column "supplier_name"}
                          {:param_name "raw_supplier_country" :source_column "supplier_country"}
                          {:param_name "raw_business_unit" :source_column "business_unit"}
                          {:param_name "raw_invoice_amount" :source_column "invoice_amount"}
                          {:param_name "raw_tax_amount" :source_column "tax_amount"}
                          {:param_name "raw_currency" :source_column "currency"}
                          {:param_name "raw_payment_terms" :source_column "payment_terms"}
                          {:param_name "raw_po_number" :source_column "po_number"}
                          {:param_name "raw_due_days" :source_column "due_days"}
                          {:param_name "raw_has_attachment" :source_column "has_attachment"}
                          {:param_name "raw_cost_center" :source_column "cost_center"}]
              :fn_lets [{:variable "invoice_key_calc" :expression "upper(trim(raw_invoice_id))"}
                        {:variable "supplier_name_calc" :expression "trim(raw_supplier_name)"}
                        {:variable "supplier_country_calc" :expression "upper(trim(raw_supplier_country))"}
                        {:variable "business_unit_calc" :expression "upper(trim(raw_business_unit))"}
                        {:variable "currency_calc" :expression "upper(trim(raw_currency))"}
                        {:variable "payment_terms_calc" :expression "upper(trim(raw_payment_terms))"}
                        {:variable "po_number_calc" :expression "trim(coalesce(raw_po_number, ''))"}
                        {:variable "cost_center_calc" :expression "trim(coalesce(raw_cost_center, ''))"}
                        {:variable "invoice_amount_calc" :expression "round(tonumber(raw_invoice_amount), 2)"}
                        {:variable "tax_amount_calc" :expression "round(tonumber(raw_tax_amount), 2)"}
                        {:variable "gross_amount_calc" :expression "round(invoice_amount_calc + tax_amount_calc, 2)"}
                        {:variable "due_days_calc" :expression "tonumber(raw_due_days)"}
                        {:variable "attachment_flag_calc" :expression "toboolean(raw_has_attachment)"}
                        {:variable "po_present_calc" :expression "not(isempty(po_number_calc))"}
                        {:variable "cost_center_present_calc" :expression "not(isempty(cost_center_calc))"}
                        {:variable "discountable_calc" :expression "contains(payment_terms_calc, '10') || due_days_calc <= 10"}]
              :fn_return ""
              :fn_outputs [{:output_name "invoice_key" :data_type "varchar" :expression "invoice_key_calc"}
                           {:output_name "supplier_name_clean" :data_type "varchar" :expression "supplier_name_calc"}
                           {:output_name "supplier_country_norm" :data_type "varchar" :expression "supplier_country_calc"}
                           {:output_name "business_unit_norm" :data_type "varchar" :expression "business_unit_calc"}
                           {:output_name "currency_norm" :data_type "varchar" :expression "currency_calc"}
                           {:output_name "payment_terms_norm" :data_type "varchar" :expression "payment_terms_calc"}
                           {:output_name "po_number_clean" :data_type "varchar" :expression "po_number_calc"}
                           {:output_name "cost_center_clean" :data_type "varchar" :expression "cost_center_calc"}
                           {:output_name "invoice_amount_num" :data_type "double" :expression "invoice_amount_calc"}
                           {:output_name "tax_amount_num" :data_type "double" :expression "tax_amount_calc"}
                           {:output_name "gross_amount" :data_type "double" :expression "gross_amount_calc"}
                           {:output_name "due_days_num" :data_type "double" :expression "due_days_calc"}
                           {:output_name "attachment_flag" :data_type "boolean" :expression "attachment_flag_calc"}
                           {:output_name "po_present" :data_type "boolean" :expression "po_present_calc"}
                           {:output_name "cost_center_present" :data_type "boolean" :expression "cost_center_present_calc"}
                           {:output_name "discountable" :data_type "boolean" :expression "discountable_calc"}]})
            (g2/save-conditional
             4
             {:cond_type "if-elif-else"
              :branches [{:condition "gross_amount >= 10000 && not(attachment_flag)" :group "ap_hold"}
                         {:condition "not(po_present) && business_unit_norm = 'OPS'" :group "po_exception"}
                         {:condition "supplier_country_norm = 'CN' || supplier_country_norm = 'RU'" :group "compliance_review"}
                         {:condition "not(cost_center_present)" :group "master_data_fix"}]
              :default_branch "policy_ready"})
            (g2/save-logic
             5
             {:fn_name "PaymentPolicy"
              :fn_params [{:param_name "validation_group" :source_column "cond_4_group"}
                          {:param_name "gross_amount_value" :source_column "gross_amount"}
                          {:param_name "currency_value" :source_column "currency_norm"}
                          {:param_name "discountable_value" :source_column "discountable"}
                          {:param_name "due_days_value" :source_column "due_days_num"}
                          {:param_name "business_unit_value" :source_column "business_unit_norm"}
                          {:param_name "supplier_country_value" :source_column "supplier_country_norm"}]
              :fn_lets [{:variable "discount_rate_calc" :expression "if(discountable_value, 0.02, 0.0)"}
                        {:variable "discount_amount_calc" :expression "round(gross_amount_value * discount_rate_calc, 2)"}
                        {:variable "scheduled_amount_calc" :expression "round(gross_amount_value - discount_amount_calc, 2)"}
                        {:variable "priority_calc" :expression "if(validation_group = 'ap_hold', 'HOLD', if(due_days_value <= 5, 'URGENT', if(due_days_value <= 15, 'PRIORITY', 'STANDARD')))"}
                        {:variable "approver_role_calc" :expression "if(gross_amount_value >= 20000, 'DIRECTOR', if(gross_amount_value >= 5000, 'MANAGER', 'AUTO'))"}
                        {:variable "payment_method_calc" :expression "if(currency_value = 'USD', 'ACH', if(currency_value = 'EUR', 'SEPA', 'WIRE'))"}
                        {:variable "processing_lane_calc" :expression "if(validation_group = 'compliance_review', 'COMPLIANCE', if(business_unit_value = 'OPS', 'OPERATIONS_AP', 'CORPORATE_AP'))"}
                        {:variable "policy_note_calc" :expression "concat(validation_group, ' | ', approver_role_calc, ' | ', payment_method_calc, ' | ', supplier_country_value)"}]
              :fn_return ""
              :fn_outputs [{:output_name "discount_rate" :data_type "double" :expression "discount_rate_calc"}
                           {:output_name "discount_amount" :data_type "double" :expression "discount_amount_calc"}
                           {:output_name "scheduled_amount" :data_type "double" :expression "scheduled_amount_calc"}
                           {:output_name "payment_priority" :data_type "varchar" :expression "priority_calc"}
                           {:output_name "approver_role" :data_type "varchar" :expression "approver_role_calc"}
                           {:output_name "payment_method" :data_type "varchar" :expression "payment_method_calc"}
                           {:output_name "processing_lane" :data_type "varchar" :expression "processing_lane_calc"}
                           {:output_name "policy_note" :data_type "varchar" :expression "policy_note_calc"}]})
            (g2/save-conditional
             6
             {:cond_type "cond"
              :branches [{:condition "cond_4_group = 'ap_hold'" :group "await_documents"}
                         {:condition "cond_4_group = 'compliance_review'" :group "sanctions_screen"}
                         {:condition "approver_role = 'DIRECTOR' && discount_rate > 0" :group "director_discount_review"}
                         {:condition "approver_role = 'AUTO' && payment_method = 'ACH'" :group "auto_pay"}]
              :default_branch "manager_queue"})
            (g2/save-logic
             7
             {:fn_name "InvoiceProjection"
              :fn_params [{:param_name "invoice_key_value" :source_column "invoice_key"}
                          {:param_name "supplier_name_value" :source_column "supplier_name_clean"}
                          {:param_name "validation_group_value" :source_column "cond_4_group"}
                          {:param_name "release_group_value" :source_column "cond_6_group"}
                          {:param_name "payment_priority_value" :source_column "payment_priority"}
                          {:param_name "processing_lane_value" :source_column "processing_lane"}
                          {:param_name "scheduled_amount_value" :source_column "scheduled_amount"}
                          {:param_name "approver_role_value" :source_column "approver_role"}
                          {:param_name "payment_method_value" :source_column "payment_method"}]
              :fn_lets [{:variable "execution_path_calc" :expression "concat(validation_group_value, ' -> ', release_group_value)"}
                        {:variable "final_action_calc" :expression "if(release_group_value = 'await_documents', 'Request missing invoice backup', if(release_group_value = 'sanctions_screen', 'Route to compliance analyst', if(release_group_value = 'director_discount_review', 'Escalate to finance director', if(release_group_value = 'auto_pay', 'Schedule automatic payment', 'Send to AP manager queue'))))"}
                        {:variable "cash_bucket_calc" :expression "if(scheduled_amount_value >= 15000, 'LARGE', if(scheduled_amount_value >= 5000, 'MEDIUM', 'SMALL'))"}
                        {:variable "queue_name_calc" :expression "concat(processing_lane_value, ':', payment_priority_value)"}
                        {:variable "audit_summary_calc" :expression "concat(invoice_key_value, ' | ', supplier_name_value, ' | ', approver_role_value, ' | ', payment_method_value, ' | ', tostring(scheduled_amount_value))"}]
              :fn_return ""
              :fn_outputs [{:output_name "execution_path" :data_type "varchar" :expression "execution_path_calc"}
                           {:output_name "final_action" :data_type "varchar" :expression "final_action_calc"}
                           {:output_name "cash_bucket" :data_type "varchar" :expression "cash_bucket_calc"}
                           {:output_name "queue_name" :data_type "varchar" :expression "queue_name_calc"}
                           {:output_name "audit_summary" :data_type "varchar" :expression "audit_summary_calc"}]})
            (g2/save-response-builder
             8
             {:status_code "200"
              :response_type "json"
              :headers ""
              :template [{:output_key "invoiceId" :source_column "invoice_id"}
                         {:output_key "invoiceKey" :source_column "invoice_key"}
                         {:output_key "supplierName" :source_column "supplier_name_clean"}
                         {:output_key "supplierCountry" :source_column "supplier_country_norm"}
                         {:output_key "businessUnit" :source_column "business_unit_norm"}
                         {:output_key "currency" :source_column "currency_norm"}
                         {:output_key "grossAmount" :source_column "gross_amount"}
                         {:output_key "discountRate" :source_column "discount_rate"}
                         {:output_key "discountAmount" :source_column "discount_amount"}
                         {:output_key "scheduledAmount" :source_column "scheduled_amount"}
                         {:output_key "validationRoute" :source_column "cond_4_group"}
                         {:output_key "releaseDecision" :source_column "cond_6_group"}
                         {:output_key "paymentPriority" :source_column "payment_priority"}
                         {:output_key "approverRole" :source_column "approver_role"}
                         {:output_key "paymentMethod" :source_column "payment_method"}
                         {:output_key "processingLane" :source_column "processing_lane"}
                         {:output_key "executionPath" :source_column "execution_path"}
                         {:output_key "finalAction" :source_column "final_action"}
                         {:output_key "cashBucket" :source_column "cash_bucket"}
                         {:output_key "queueName" :source_column "queue_name"}
                         {:output_key "auditSummary" :source_column "audit_summary"}
                         {:output_key "validationCondition" :source_column "cond_4_condition"}
                         {:output_key "releaseCondition" :source_column "cond_6_condition"}]}))
        saved (insert-demo! graph)
        gid   (get-in saved [:a :id])]
    {:name "Invoice Eligibility + Cleansing Demo"
     :id gid
     :sample-request {:invoice_id "INV-88421"
                      :supplier_name "Northwind Components"
                      :supplier_country "CN"
                      :business_unit "OPS"
                      :invoice_amount "18450.75"
                      :tax_amount "922.54"
                      :currency "usd"
                      :payment_terms "2% 10 NET 30"
                      :po_number ""
                      :due_days "8"
                      :has_attachment "false"
                      :cost_center ""}
     :sample-response (sample-run gid {:invoice_id "INV-88421"
                                       :supplier_name "Northwind Components"
                                       :supplier_country "CN"
                                       :business_unit "OPS"
                                       :invoice_amount "18450.75"
                                       :tax_amount "922.54"
                                       :currency "usd"
                                       :payment_terms "2% 10 NET 30"
                                       :po_number ""
                                       :due_days "8"
                                       :has_attachment "false"
                                       :cost_center ""})}))

(defn- create-customer-mastering-demo!
  []
  (let [graph
        (-> (empty-graph "Customer Mastering + Exception Queue Demo")
            (g2/add-node {:name "customer_input" :btype "Ep" :tcols {}})
            (g2/add-node {:name "standardize_customer" :btype "Fu" :tcols {}})
            (g2/add-node {:name "quality_gate" :btype "C" :tcols {}})
            (g2/add-node {:name "mastering_policy" :btype "Fu" :tcols {}})
            (g2/add-node {:name "publish_route" :btype "C" :tcols {}})
            (g2/add-node {:name "customer_projection" :btype "Fu" :tcols {}})
            (g2/add-node {:name "response_builder" :btype "Rb" :tcols {}})
            (g2/add-edges [[2 3] [3 4] [4 5] [5 6] [6 7] [7 8] [8 1]])
            (g2/save-endpoint
             2
             {:http_method "GET"
              :route_path "/demo/customer-mastering"
              :response_format "json"
              :description "A customer mastering pipeline showing standardization, quality rules, exception routing, and final publish decisions without Python transform code."
              :query_params [{:param_name "customer_id" :data_type "varchar" :required true}
                             {:param_name "first_name" :data_type "varchar" :required true}
                             {:param_name "last_name" :data_type "varchar" :required true}
                             {:param_name "email" :data_type "varchar" :required false}
                             {:param_name "phone" :data_type "varchar" :required false}
                             {:param_name "state" :data_type "varchar" :required false}
                             {:param_name "country" :data_type "varchar" :required true}
                             {:param_name "customer_type" :data_type "varchar" :required true}
                             {:param_name "lifetime_value" :data_type "double" :required true}
                             {:param_name "orders_count" :data_type "integer" :required true}
                             {:param_name "source_system" :data_type "varchar" :required true}
                             {:param_name "opt_in" :data_type "boolean" :required false}]
              :path_params []
              :body_schema []})
            (g2/save-logic
             3
             {:fn_name "StandardizeCustomer"
              :fn_params [{:param_name "raw_customer_id" :source_column "customer_id"}
                          {:param_name "raw_first_name" :source_column "first_name"}
                          {:param_name "raw_last_name" :source_column "last_name"}
                          {:param_name "raw_email" :source_column "email"}
                          {:param_name "raw_phone" :source_column "phone"}
                          {:param_name "raw_state" :source_column "state"}
                          {:param_name "raw_country" :source_column "country"}
                          {:param_name "raw_customer_type" :source_column "customer_type"}
                          {:param_name "raw_lifetime_value" :source_column "lifetime_value"}
                          {:param_name "raw_orders_count" :source_column "orders_count"}
                          {:param_name "raw_source_system" :source_column "source_system"}
                          {:param_name "raw_opt_in" :source_column "opt_in"}]
              :fn_lets [{:variable "customer_key_calc" :expression "upper(trim(raw_customer_id))"}
                        {:variable "first_name_clean_calc" :expression "trim(raw_first_name)"}
                        {:variable "last_name_clean_calc" :expression "trim(raw_last_name)"}
                        {:variable "full_name_calc" :expression "concat(first_name_clean_calc, ' ', last_name_clean_calc)"}
                        {:variable "email_clean_calc" :expression "lower(trim(coalesce(raw_email, '')))"}
                        {:variable "phone_clean_calc" :expression "replace(replace(replace(replace(trim(coalesce(raw_phone, '')), '(', ''), ')', ''), '-', ''), ' ', '')"}
                        {:variable "state_norm_calc" :expression "upper(trim(coalesce(raw_state, '')))"}
                        {:variable "country_norm_calc" :expression "upper(trim(raw_country))"}
                        {:variable "customer_type_norm_calc" :expression "upper(trim(raw_customer_type))"}
                        {:variable "source_system_norm_calc" :expression "upper(trim(raw_source_system))"}
                        {:variable "lifetime_value_calc" :expression "round(tonumber(raw_lifetime_value), 2)"}
                        {:variable "orders_count_calc" :expression "tonumber(raw_orders_count)"}
                        {:variable "opt_in_flag_calc" :expression "toboolean(raw_opt_in)"}
                        {:variable "email_present_calc" :expression "not(isempty(email_clean_calc))"}
                        {:variable "phone_present_calc" :expression "not(isempty(phone_clean_calc))"}
                        {:variable "email_valid_calc" :expression "contains(email_clean_calc, '@') && contains(email_clean_calc, '.')"}
                        {:variable "phone_valid_calc" :expression "length(phone_clean_calc) >= 10"}
                        {:variable "contact_score_calc" :expression "if(email_present_calc && email_valid_calc, 50, 0) + if(phone_present_calc && phone_valid_calc, 50, 0)"}
                        {:variable "value_segment_calc" :expression "if(lifetime_value_calc >= 10000, 'STRATEGIC', if(lifetime_value_calc >= 2500, 'GROWTH', 'CORE'))"}]
              :fn_return ""
              :fn_outputs [{:output_name "customer_key" :data_type "varchar" :expression "customer_key_calc"}
                           {:output_name "first_name_clean" :data_type "varchar" :expression "first_name_clean_calc"}
                           {:output_name "last_name_clean" :data_type "varchar" :expression "last_name_clean_calc"}
                           {:output_name "full_name" :data_type "varchar" :expression "full_name_calc"}
                           {:output_name "email_clean" :data_type "varchar" :expression "email_clean_calc"}
                           {:output_name "phone_clean" :data_type "varchar" :expression "phone_clean_calc"}
                           {:output_name "state_norm" :data_type "varchar" :expression "state_norm_calc"}
                           {:output_name "country_norm" :data_type "varchar" :expression "country_norm_calc"}
                           {:output_name "customer_type_norm" :data_type "varchar" :expression "customer_type_norm_calc"}
                           {:output_name "source_system_norm" :data_type "varchar" :expression "source_system_norm_calc"}
                           {:output_name "lifetime_value_num" :data_type "double" :expression "lifetime_value_calc"}
                           {:output_name "orders_count_num" :data_type "double" :expression "orders_count_calc"}
                           {:output_name "opt_in_flag" :data_type "boolean" :expression "opt_in_flag_calc"}
                           {:output_name "email_present" :data_type "boolean" :expression "email_present_calc"}
                           {:output_name "phone_present" :data_type "boolean" :expression "phone_present_calc"}
                           {:output_name "email_valid" :data_type "boolean" :expression "email_valid_calc"}
                           {:output_name "phone_valid" :data_type "boolean" :expression "phone_valid_calc"}
                           {:output_name "contact_score" :data_type "double" :expression "contact_score_calc"}
                           {:output_name "value_segment" :data_type "varchar" :expression "value_segment_calc"}]})
            (g2/save-conditional
             4
             {:cond_type "if-elif-else"
              :branches [{:condition "not(email_valid) && not(phone_valid)" :group "reject_contact"}
                         {:condition "customer_type_norm = 'B2B' && isempty(state_norm)" :group "enrich_region"}
                         {:condition "source_system_norm = 'LEGACY_CRM' && contact_score < 100" :group "steward_review"}
                         {:condition "country_norm <> 'USA' && country_norm <> 'CANADA'" :group "cross_border_review"}]
              :default_branch "quality_pass"})
            (g2/save-logic
             5
             {:fn_name "MasteringPolicy"
              :fn_params [{:param_name "quality_group" :source_column "cond_4_group"}
                          {:param_name "customer_type_value" :source_column "customer_type_norm"}
                          {:param_name "value_segment_value" :source_column "value_segment"}
                          {:param_name "lifetime_value_value" :source_column "lifetime_value_num"}
                          {:param_name "orders_count_value" :source_column "orders_count_num"}
                          {:param_name "contact_score_value" :source_column "contact_score"}
                          {:param_name "opt_in_value" :source_column "opt_in_flag"}
                          {:param_name "country_value" :source_column "country_norm"}
                          {:param_name "source_system_value" :source_column "source_system_norm"}]
              :fn_lets [{:variable "golden_status_calc" :expression "if(quality_group = 'reject_contact', 'BLOCKED', if(quality_group = 'quality_pass', 'MASTER_READY', 'PENDING_REVIEW'))"}
                        {:variable "survivorship_bucket_calc" :expression "if(source_system_value = 'ERP', 'FINANCE_TRUSTED', if(source_system_value = 'ECOM', 'DIGITAL_TRUSTED', 'STANDARD_MERGE'))"}
                        {:variable "service_tier_calc" :expression "if(value_segment_value = 'STRATEGIC', 'WHITE_GLOVE', if(value_segment_value = 'GROWTH', 'PRIORITY', 'STANDARD'))"}
                        {:variable "engagement_score_calc" :expression "round(contact_score_value + min(50, orders_count_value * 5) + if(opt_in_value, 20, 0), 2)"}
                        {:variable "publish_domain_calc" :expression "if(customer_type_value = 'B2B', 'ACCOUNT', 'CONSUMER')"}
                        {:variable "steward_queue_calc" :expression "if(quality_group = 'quality_pass', 'AUTO', if(quality_group = 'enrich_region', 'DATA_ENRICHMENT', if(quality_group = 'cross_border_review', 'COMPLIANCE_STEWARD', 'CUSTOMER_STEWARD')))"}
                        {:variable "master_note_calc" :expression "concat(quality_group, ' | ', survivorship_bucket_calc, ' | ', service_tier_calc, ' | ', country_value)"}]
              :fn_return ""
              :fn_outputs [{:output_name "golden_status" :data_type "varchar" :expression "golden_status_calc"}
                           {:output_name "survivorship_bucket" :data_type "varchar" :expression "survivorship_bucket_calc"}
                           {:output_name "service_tier" :data_type "varchar" :expression "service_tier_calc"}
                           {:output_name "engagement_score" :data_type "double" :expression "engagement_score_calc"}
                           {:output_name "publish_domain" :data_type "varchar" :expression "publish_domain_calc"}
                           {:output_name "steward_queue" :data_type "varchar" :expression "steward_queue_calc"}
                           {:output_name "master_note" :data_type "varchar" :expression "master_note_calc"}]})
            (g2/save-conditional
             6
             {:cond_type "cond"
              :branches [{:condition "golden_status = 'BLOCKED'" :group "do_not_publish"}
                         {:condition "steward_queue <> 'AUTO'" :group "queue_for_steward"}
                         {:condition "publish_domain = 'ACCOUNT' && service_tier = 'WHITE_GLOVE'" :group "publish_account_priority"}
                         {:condition "publish_domain = 'CONSUMER' && engagement_score >= 120" :group "publish_marketing_ready"}]
              :default_branch "publish_standard"})
            (g2/save-logic
             7
             {:fn_name "CustomerProjection"
              :fn_params [{:param_name "customer_key_value" :source_column "customer_key"}
                          {:param_name "full_name_value" :source_column "full_name"}
                          {:param_name "quality_group_value" :source_column "cond_4_group"}
                          {:param_name "publish_group_value" :source_column "cond_6_group"}
                          {:param_name "golden_status_value" :source_column "golden_status"}
                          {:param_name "service_tier_value" :source_column "service_tier"}
                          {:param_name "steward_queue_value" :source_column "steward_queue"}
                          {:param_name "engagement_score_value" :source_column "engagement_score"}
                          {:param_name "publish_domain_value" :source_column "publish_domain"}]
              :fn_lets [{:variable "execution_path_calc" :expression "concat(quality_group_value, ' -> ', publish_group_value)"}
                        {:variable "final_action_calc" :expression "if(publish_group_value = 'do_not_publish', 'Suppress record from downstream systems', if(publish_group_value = 'queue_for_steward', 'Send record to data stewardship worklist', if(publish_group_value = 'publish_account_priority', 'Publish to account hub with priority SLA', if(publish_group_value = 'publish_marketing_ready', 'Publish to CRM and marketing audience', 'Publish to standard customer domain'))))"}
                        {:variable "target_queue_calc" :expression "concat(steward_queue_value, ':', service_tier_value)"}
                        {:variable "profile_band_calc" :expression "if(engagement_score_value >= 140, 'ELITE', if(engagement_score_value >= 90, 'ACTIVE', 'BASIC'))"}
                        {:variable "audit_summary_calc" :expression "concat(customer_key_value, ' | ', full_name_value, ' | ', golden_status_value, ' | ', publish_domain_value, ' | score=', tostring(engagement_score_value))"}]
              :fn_return ""
              :fn_outputs [{:output_name "execution_path" :data_type "varchar" :expression "execution_path_calc"}
                           {:output_name "final_action" :data_type "varchar" :expression "final_action_calc"}
                           {:output_name "target_queue" :data_type "varchar" :expression "target_queue_calc"}
                           {:output_name "profile_band" :data_type "varchar" :expression "profile_band_calc"}
                           {:output_name "audit_summary" :data_type "varchar" :expression "audit_summary_calc"}]})
            (g2/save-response-builder
             8
             {:status_code "200"
              :response_type "json"
              :headers ""
              :template [{:output_key "customerId" :source_column "customer_id"}
                         {:output_key "customerKey" :source_column "customer_key"}
                         {:output_key "fullName" :source_column "full_name"}
                         {:output_key "email" :source_column "email_clean"}
                         {:output_key "phone" :source_column "phone_clean"}
                         {:output_key "state" :source_column "state_norm"}
                         {:output_key "country" :source_column "country_norm"}
                         {:output_key "customerType" :source_column "customer_type_norm"}
                         {:output_key "valueSegment" :source_column "value_segment"}
                         {:output_key "contactScore" :source_column "contact_score"}
                         {:output_key "qualityRoute" :source_column "cond_4_group"}
                         {:output_key "publishDecision" :source_column "cond_6_group"}
                         {:output_key "goldenStatus" :source_column "golden_status"}
                         {:output_key "survivorshipBucket" :source_column "survivorship_bucket"}
                         {:output_key "serviceTier" :source_column "service_tier"}
                         {:output_key "engagementScore" :source_column "engagement_score"}
                         {:output_key "publishDomain" :source_column "publish_domain"}
                         {:output_key "stewardQueue" :source_column "steward_queue"}
                         {:output_key "executionPath" :source_column "execution_path"}
                         {:output_key "finalAction" :source_column "final_action"}
                         {:output_key "targetQueue" :source_column "target_queue"}
                         {:output_key "profileBand" :source_column "profile_band"}
                         {:output_key "auditSummary" :source_column "audit_summary"}
                         {:output_key "qualityCondition" :source_column "cond_4_condition"}
                         {:output_key "publishCondition" :source_column "cond_6_condition"}]}))
        saved (insert-demo! graph)
        gid   (get-in saved [:a :id])]
    {:name "Customer Mastering + Exception Queue Demo"
     :id gid
     :sample-request {:customer_id "C-77881"
                      :first_name "Maya"
                      :last_name "Thompson"
                      :email "MAYA.THOMPSON@EXAMPLE.COM"
                      :phone "(415) 555-9944"
                      :state ""
                      :country "USA"
                      :customer_type "B2B"
                      :lifetime_value "12850.40"
                      :orders_count "11"
                      :source_system "legacy_crm"
                      :opt_in "true"}
     :sample-response (sample-run gid {:customer_id "C-77881"
                                       :first_name "Maya"
                                       :last_name "Thompson"
                                       :email "MAYA.THOMPSON@EXAMPLE.COM"
                                       :phone "(415) 555-9944"
                                       :state ""
                                       :country "USA"
                                       :customer_type "B2B"
                                       :lifetime_value "12850.40"
                                       :orders_count "11"
                                       :source_system "legacy_crm"
                                       :opt_in "true"})}))

(defn- create-executive-transform-demo!
  []
  (let [graph
        (-> (empty-graph "Executive Transform Demo")
            (g2/add-node {:name "lead_input" :btype "Ep" :tcols {}})
            (g2/add-node {:name "score_and_segment" :btype "Fu" :tcols {}})
            (g2/add-node {:name "decision_gate" :btype "C" :tcols {}})
            (g2/add-node {:name "action_projection" :btype "Fu" :tcols {}})
            (g2/add-node {:name "response_builder" :btype "Rb" :tcols {}})
            (g2/add-edges [[2 3] [3 4] [4 5] [5 6] [6 1]])
            (g2/save-endpoint
             2
             {:http_method "GET"
              :route_path "/demo/executive-transform"
              :response_format "json"
              :description "A short executive demo showing a complete data transform pipeline with assignments, scoring, branching, and final actioning without Python."
              :query_params [{:param_name "lead_id" :data_type "varchar" :required true}
                             {:param_name "company" :data_type "varchar" :required true}
                             {:param_name "country" :data_type "varchar" :required true}
                             {:param_name "annual_revenue" :data_type "double" :required true}
                             {:param_name "employee_count" :data_type "integer" :required true}
                             {:param_name "intent_score" :data_type "double" :required true}
                             {:param_name "source" :data_type "varchar" :required true}]
              :path_params []
              :body_schema []})
            (g2/save-logic
             3
             {:fn_name "LeadScoring"
              :fn_params [{:param_name "raw_lead_id" :source_column "lead_id"}
                          {:param_name "raw_company" :source_column "company"}
                          {:param_name "raw_country" :source_column "country"}
                          {:param_name "raw_revenue" :source_column "annual_revenue"}
                          {:param_name "raw_employees" :source_column "employee_count"}
                          {:param_name "raw_intent" :source_column "intent_score"}
                          {:param_name "raw_source" :source_column "source"}]
              :fn_lets [{:variable "lead_key_calc" :expression "upper(trim(raw_lead_id))"}
                        {:variable "company_clean_calc" :expression "trim(raw_company)"}
                        {:variable "country_norm_calc" :expression "upper(trim(raw_country))"}
                        {:variable "revenue_calc" :expression "round(tonumber(raw_revenue), 2)"}
                        {:variable "employees_calc" :expression "tonumber(raw_employees)"}
                        {:variable "intent_calc" :expression "round(tonumber(raw_intent), 2)"}
                        {:variable "source_norm_calc" :expression "upper(trim(raw_source))"}
                        {:variable "fit_score_calc" :expression "if(revenue_calc >= 10000000, 50, if(revenue_calc >= 1000000, 35, 20)) + if(employees_calc >= 1000, 25, if(employees_calc >= 200, 15, 5))"}
                        {:variable "total_score_calc" :expression "round(fit_score_calc + intent_calc, 2)"}
                        {:variable "segment_calc" :expression "if(total_score_calc >= 120, 'EXEC_PRIORITY', if(total_score_calc >= 80, 'SALES_READY', 'NURTURE'))"}]
              :fn_return ""
              :fn_outputs [{:output_name "lead_key" :data_type "varchar" :expression "lead_key_calc"}
                           {:output_name "company_clean" :data_type "varchar" :expression "company_clean_calc"}
                           {:output_name "country_norm" :data_type "varchar" :expression "country_norm_calc"}
                           {:output_name "source_norm" :data_type "varchar" :expression "source_norm_calc"}
                           {:output_name "fit_score" :data_type "double" :expression "fit_score_calc"}
                           {:output_name "total_score" :data_type "double" :expression "total_score_calc"}
                           {:output_name "segment" :data_type "varchar" :expression "segment_calc"}]})
            (g2/save-conditional
             4
             {:cond_type "if-elif-else"
              :branches [{:condition "segment = 'EXEC_PRIORITY' && country_norm = 'USA'" :group "assign_ae"}
                         {:condition "segment = 'SALES_READY'" :group "assign_sdr"}
                         {:condition "source_norm = 'PARTNER'" :group "partner_followup"}]
              :default_branch "nurture_stream"})
            (g2/save-logic
             5
             {:fn_name "LeadAction"
              :fn_params [{:param_name "company_value" :source_column "company_clean"}
                          {:param_name "segment_value" :source_column "segment"}
                          {:param_name "route_value" :source_column "cond_4_group"}
                          {:param_name "score_value" :source_column "total_score"}]
              :fn_lets [{:variable "owner_calc" :expression "if(route_value = 'assign_ae', 'Account Executive', if(route_value = 'assign_sdr', 'Sales Development', if(route_value = 'partner_followup', 'Partner Team', 'Marketing Automation')))"}
                        {:variable "next_step_calc" :expression "if(route_value = 'assign_ae', 'Schedule executive discovery call', if(route_value = 'assign_sdr', 'Create outbound task sequence', if(route_value = 'partner_followup', 'Notify partner manager', 'Enroll in nurture campaign')))"}
                        {:variable "summary_calc" :expression "concat(company_value, ' | ', segment_value, ' | score=', tostring(score_value))"}]
              :fn_return ""
              :fn_outputs [{:output_name "recommended_owner" :data_type "varchar" :expression "owner_calc"}
                           {:output_name "next_step" :data_type "varchar" :expression "next_step_calc"}
                           {:output_name "summary" :data_type "varchar" :expression "summary_calc"}]})
            (g2/save-response-builder
             6
             {:status_code "200"
              :response_type "json"
              :headers ""
              :template [{:output_key "leadId" :source_column "lead_id"}
                         {:output_key "leadKey" :source_column "lead_key"}
                         {:output_key "company" :source_column "company_clean"}
                         {:output_key "country" :source_column "country_norm"}
                         {:output_key "source" :source_column "source_norm"}
                         {:output_key "fitScore" :source_column "fit_score"}
                         {:output_key "totalScore" :source_column "total_score"}
                         {:output_key "segment" :source_column "segment"}
                         {:output_key "route" :source_column "cond_4_group"}
                         {:output_key "matchedCondition" :source_column "cond_4_condition"}
                         {:output_key "recommendedOwner" :source_column "recommended_owner"}
                         {:output_key "nextStep" :source_column "next_step"}
                         {:output_key "summary" :source_column "summary"}]}))
        saved (insert-demo! graph)
        gid   (get-in saved [:a :id])]
    {:name "Executive Transform Demo"
     :id gid
     :sample-request {:lead_id "L-1009"
                      :company "Helio Freight"
                      :country "USA"
                      :annual_revenue "18500000"
                      :employee_count "1400"
                      :intent_score "48"
                      :source "web"}
     :sample-response (sample-run gid {:lead_id "L-1009"
                                       :company "Helio Freight"
                                       :country "USA"
                                       :annual_revenue "18500000"
                                       :employee_count "1400"
                                       :intent_score "48"
                                       :source "web"})}))

(defn -main
  [& _args]
  (doseq [{:keys [name id sample-request sample-response]}
          [(create-if-elif-demo!)
           (create-case-demo!)
           (create-pattern-demo!)
           (create-plsql-style-transform-demo!)
           (create-invoice-eligibility-demo!)
           (create-customer-mastering-demo!)
           (create-executive-transform-demo!)]]
    (println (str "Created graph: " name))
    (println (str "Graph ID: " id))
    (println "Sample request:")
    (println (json/generate-string sample-request))
    (println "Sample response:")
    (println (json/generate-string sample-response {:pretty true}))
    (println)))
