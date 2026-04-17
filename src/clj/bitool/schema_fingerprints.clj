(ns bitool.schema-fingerprints
  "Known SaaS schema fingerprints for auto-recognition.
   When Bitool connects to a warehouse and sees Fivetran/Airbyte output tables,
   it returns curated business descriptions instead of LLM-guessing them.
   This is the 'auto-detect faces' layer — no connectors, just recognition."
  (:require [clojure.string :as str]))

;; ─────────────────────────────────────────────────────────────────────
;; Catalog Format
;; Each entry:
;;   :source      — canonical name shown to user
;;   :marker-tables — tables whose presence confirms this source (need 2+ matches)
;;   :tables      — {table-name {:desc "..." :columns {col-name "desc"}}}
;;   :tips        — vec of gotcha strings injected into the ISL prompt
;; ─────────────────────────────────────────────────────────────────────

(def catalog
  {"stripe"
   {:source "Stripe"
    :marker-tables #{"stripe_charges" "stripe_customers" "stripe_payment_intents"
                     "charge" "customer" "payment_intent"
                     "stripe__charges" "stripe__customers"}
    :tables
    {"stripe_charges"
     {:desc "Stripe payment charges — one row per charge attempt"
      :columns {"id"                    "Stripe charge ID (ch_...)"
                "amount"                "Amount in cents — divide by 100 for dollars"
                "amount_refunded"       "Refunded amount in cents"
                "currency"              "ISO currency code (usd, eur, etc.)"
                "status"                "succeeded, pending, or failed"
                "created"               "Unix timestamp of charge creation"
                "customer_id"           "FK to stripe_customers.id"
                "payment_intent_id"     "FK to stripe_payment_intents.id"
                "balance_transaction_id" "FK to stripe_balance_transactions.id"
                "description"           "Charge description set by merchant"
                "failure_code"          "Failure reason code (null if succeeded)"
                "captured"              "Whether the charge was captured"
                "paid"                  "Whether the charge was paid"
                "refunded"              "Whether the charge was fully refunded"}}
     "charge"
     {:desc "Stripe payment charges (Fivetran naming) — one row per charge attempt"
      :columns {"id"                "Stripe charge ID (ch_...)"
                "amount"            "Amount in cents — divide by 100 for dollars"
                "amount_refunded"   "Refunded amount in cents"
                "currency"          "ISO currency code"
                "status"            "succeeded, pending, or failed"
                "created"           "Unix timestamp"
                "customer_id"       "FK to customer.id"}}
     "stripe_customers"
     {:desc "Stripe customer records — one row per customer"
      :columns {"id"           "Stripe customer ID (cus_...)"
                "email"        "Customer email address"
                "name"         "Customer display name"
                "created"      "Unix timestamp of customer creation"
                "currency"     "Default currency"
                "delinquent"   "Whether customer has unpaid invoices"
                "description"  "Internal description"
                "livemode"     "True if live mode, false if test mode"}}
     "customer"
     {:desc "Stripe customer records (Fivetran naming)"
      :columns {"id"           "Stripe customer ID (cus_...)"
                "email"        "Customer email address"
                "name"         "Customer display name"
                "created"      "Unix timestamp"
                "delinquent"   "Has unpaid invoices"}}
     "stripe_payment_intents"
     {:desc "Stripe payment intents — tracks a payment from creation to completion"
      :columns {"id"                    "Payment intent ID (pi_...)"
                "amount"                "Intended amount in cents"
                "currency"              "ISO currency code"
                "status"                "requires_payment_method, succeeded, canceled, etc."
                "customer_id"           "FK to stripe_customers.id"
                "created"               "Unix timestamp"
                "payment_method_id"     "FK to payment method used"}}
     "payment_intent"
     {:desc "Stripe payment intents (Fivetran naming)"
      :columns {"id"          "Payment intent ID (pi_...)"
                "amount"      "Amount in cents"
                "currency"    "ISO currency code"
                "status"      "requires_payment_method, succeeded, canceled, etc."
                "customer_id" "FK to customer.id"
                "created"     "Unix timestamp"}}
     "stripe_subscriptions"
     {:desc "Stripe subscriptions — recurring billing"
      :columns {"id"                     "Subscription ID (sub_...)"
                "customer_id"            "FK to stripe_customers.id"
                "status"                 "active, past_due, canceled, trialing, etc."
                "current_period_start"   "Unix timestamp of current billing period start"
                "current_period_end"     "Unix timestamp of current billing period end"
                "created"                "Unix timestamp of creation"
                "cancel_at_period_end"   "Whether subscription cancels at period end"}}
     "subscription"
     {:desc "Stripe subscriptions (Fivetran naming)"
      :columns {"id"            "Subscription ID (sub_...)"
                "customer_id"   "FK to customer.id"
                "status"        "active, past_due, canceled, trialing"
                "created"       "Unix timestamp"}}
     "stripe_invoices"
     {:desc "Stripe invoices — billing documents"
      :columns {"id"              "Invoice ID (in_...)"
                "customer_id"     "FK to stripe_customers.id"
                "subscription_id" "FK to stripe_subscriptions.id"
                "total"           "Total in cents"
                "amount_due"      "Amount due in cents"
                "amount_paid"     "Amount paid in cents"
                "currency"        "ISO currency code"
                "status"          "draft, open, paid, void, uncollectible"
                "created"         "Unix timestamp"
                "due_date"        "Unix timestamp of due date"}}
     "invoice"
     {:desc "Stripe invoices (Fivetran naming)"
      :columns {"id"            "Invoice ID (in_...)"
                "customer_id"   "FK to customer.id"
                "total"         "Total in cents"
                "status"        "draft, open, paid, void, uncollectible"
                "created"       "Unix timestamp"}}
     "stripe_balance_transactions"
     {:desc "Stripe balance transactions — ledger entries for money movement"
      :columns {"id"          "Balance transaction ID (txn_...)"
                "amount"      "Net amount in cents"
                "fee"         "Stripe fee in cents"
                "net"         "Net after fees in cents"
                "currency"    "ISO currency code"
                "type"        "charge, refund, payout, adjustment, etc."
                "created"     "Unix timestamp"
                "source"      "ID of originating object (charge, refund, etc.)"}}}
    :tips ["Stripe amounts are in cents — divide by 100 for dollars"
           "Stripe timestamps are Unix epoch seconds — not ISO dates"
           "customer_id links charges, subscriptions, and invoices to customers"]}

   "hubspot"
   {:source "HubSpot"
    :marker-tables #{"hubspot_contacts" "hubspot_companies" "hubspot_deals"
                     "contact" "company" "deal"
                     "hubspot__contacts" "hubspot__companies"}
    :tables
    {"hubspot_contacts"
     {:desc "HubSpot contacts — people in the CRM"
      :columns {"id"              "HubSpot contact ID"
                "email"           "Primary email address"
                "firstname"       "First name"
                "lastname"        "Last name"
                "lifecyclestage"  "subscriber, lead, mql, sql, opportunity, customer, evangelist"
                "createdate"      "ISO timestamp of creation"
                "lastmodifieddate" "ISO timestamp of last update"
                "associatedcompanyid" "FK to hubspot_companies.id"
                "hs_lead_status"  "Lead status (new, open, in_progress, etc.)"}}
     "hubspot_companies"
     {:desc "HubSpot companies — organizations in the CRM"
      :columns {"id"             "HubSpot company ID"
                "name"           "Company name"
                "domain"         "Company website domain"
                "industry"       "Industry classification"
                "numberofemployees" "Employee count (may be a range string)"
                "annualrevenue"  "Annual revenue (numeric, in company currency)"
                "lifecyclestage" "subscriber, lead, mql, sql, opportunity, customer"
                "createdate"     "ISO timestamp"}}
     "hubspot_deals"
     {:desc "HubSpot deals — sales opportunities in the pipeline"
      :columns {"id"              "HubSpot deal ID"
                "dealname"        "Deal name / title"
                "amount"          "Deal value in pipeline currency"
                "dealstage"       "Pipeline stage ID — join with hubspot_deal_pipelines for label"
                "pipeline"        "Pipeline ID"
                "closedate"       "Expected close date (ISO)"
                "createdate"      "ISO timestamp of creation"
                "hs_is_closed"    "true if deal is closed (won or lost)"
                "hs_is_closed_won" "true if deal is closed-won"
                "associatedcompanyid" "FK to hubspot_companies.id"}}
     "hubspot_deal_pipelines"
     {:desc "HubSpot deal pipeline stages — lookup for dealstage IDs"
      :columns {"pipeline_id" "Pipeline ID"
                "stage_id"    "Stage ID (matches deals.dealstage)"
                "label"       "Human-readable stage label"
                "display_order" "Sort order in pipeline"
                "probability"   "Win probability (0-1)"}}}
    :tips ["HubSpot dealstage is an ID not a label — join with deal_pipelines for display names"
           "lifecyclestage values are lowercase strings not enums"
           "associatedcompanyid links contacts and deals to companies"]}

   "salesforce"
   {:source "Salesforce"
    :marker-tables #{"sf_account" "sf_opportunity" "sf_contact" "sf_lead"
                     "account" "opportunity" "contact" "lead"
                     "salesforce_accounts" "salesforce_opportunities"}
    :tables
    {"sf_account"
     {:desc "Salesforce accounts — companies/organizations"
      :columns {"id"           "Salesforce Account ID (18-char)"
                "name"         "Account name"
                "type"         "Customer, Prospect, Partner, etc."
                "industry"     "Industry picklist value"
                "annualrevenue" "Annual revenue (numeric)"
                "numberofemployees" "Employee count"
                "billingcountry"   "Billing country"
                "ownerid"          "FK to user who owns the account"
                "createddate"      "ISO timestamp"
                "isdeleted"        "Soft-delete flag"}}
     "account"
     {:desc "Salesforce accounts (Fivetran naming)"
      :columns {"id" "Salesforce Account ID" "name" "Account name"
                "type" "Customer, Prospect, Partner" "industry" "Industry"
                "annual_revenue" "Annual revenue" "is_deleted" "Soft-delete flag"}}
     "sf_opportunity"
     {:desc "Salesforce opportunities — deals in the pipeline"
      :columns {"id"           "Opportunity ID (18-char)"
                "name"         "Opportunity name"
                "amount"       "Deal amount in currency"
                "stagename"    "Pipeline stage label"
                "closedate"    "Expected close date (ISO)"
                "isclosed"     "True if closed (won or lost)"
                "iswon"        "True if closed-won"
                "accountid"    "FK to sf_account.id"
                "ownerid"      "FK to owner user"
                "probability"  "Win probability (0-100)"
                "createddate"  "ISO timestamp"}}
     "opportunity"
     {:desc "Salesforce opportunities (Fivetran naming)"
      :columns {"id" "Opportunity ID" "name" "Opportunity name"
                "amount" "Deal amount" "stage_name" "Pipeline stage"
                "close_date" "Expected close date" "is_won" "Closed-won flag"
                "account_id" "FK to account.id"}}
     "sf_contact"
     {:desc "Salesforce contacts — people associated with accounts"
      :columns {"id"        "Contact ID" "firstname" "First name"
                "lastname"  "Last name" "email" "Email address"
                "accountid" "FK to sf_account.id"
                "title"     "Job title" "createddate" "ISO timestamp"}}
     "sf_lead"
     {:desc "Salesforce leads — unqualified prospects"
      :columns {"id"        "Lead ID" "firstname" "First name"
                "lastname"  "Last name" "email" "Email"
                "company"   "Company name" "status" "Lead status"
                "isconverted" "Whether lead was converted to contact/opportunity"
                "createddate" "ISO timestamp"}}}
    :tips ["Salesforce IDs are 18-character case-insensitive strings"
           "isdeleted=true rows are soft-deleted — usually exclude them"
           "stagename is a label (not an ID) in Salesforce unlike HubSpot"]}

   "shopify"
   {:source "Shopify"
    :marker-tables #{"shopify_orders" "shopify_products" "shopify_customers"
                     "order" "product" "shopify__orders"}
    :tables
    {"shopify_orders"
     {:desc "Shopify orders — one row per order placed"
      :columns {"id"                "Shopify order ID"
                "order_number"      "Human-readable order number (#1001, etc.)"
                "email"             "Customer email"
                "total_price"       "Total in shop currency (already in dollars, not cents)"
                "subtotal_price"    "Subtotal before tax/shipping"
                "total_tax"         "Tax amount"
                "total_discounts"   "Discount amount"
                "currency"          "ISO currency code"
                "financial_status"  "pending, paid, refunded, voided, partially_refunded"
                "fulfillment_status" "null (unfulfilled), fulfilled, partial"
                "created_at"        "ISO timestamp"
                "customer_id"       "FK to shopify_customers.id"
                "cancelled_at"      "ISO timestamp if cancelled, else null"}}
     "shopify_products"
     {:desc "Shopify products — catalog items"
      :columns {"id"          "Shopify product ID"
                "title"       "Product title"
                "vendor"      "Vendor/brand name"
                "product_type" "Product category"
                "created_at"  "ISO timestamp"
                "status"      "active, archived, draft"}}
     "shopify_customers"
     {:desc "Shopify customers — people who have placed orders"
      :columns {"id"             "Shopify customer ID"
                "email"          "Email address"
                "first_name"     "First name"
                "last_name"      "Last name"
                "orders_count"   "Total order count"
                "total_spent"    "Lifetime spend (dollars, not cents)"
                "created_at"     "ISO timestamp"}}
     "shopify_order_line_items"
     {:desc "Shopify line items — individual products within an order"
      :columns {"id"          "Line item ID"
                "order_id"    "FK to shopify_orders.id"
                "product_id"  "FK to shopify_products.id"
                "variant_id"  "Product variant ID"
                "title"       "Product title at time of purchase"
                "quantity"    "Units ordered"
                "price"       "Unit price (dollars, not cents)"}}}
    :tips ["Shopify amounts are in dollars (not cents) unlike Stripe"
           "fulfillment_status null means unfulfilled — filter with IS NULL"
           "order_number is the customer-facing number; id is the internal ID"]}

   "quickbooks"
   {:source "QuickBooks"
    :marker-tables #{"qb_invoices" "qb_customers" "qb_payments"
                     "invoice" "quickbooks_invoices"}
    :tables
    {"qb_invoices"
     {:desc "QuickBooks invoices — billing documents sent to customers"
      :columns {"id"           "QuickBooks invoice ID"
                "customer_id"  "FK to qb_customers.id"
                "total_amount" "Invoice total (dollars)"
                "balance"      "Remaining balance (dollars)"
                "due_date"     "Due date (ISO)"
                "txn_date"     "Transaction date (ISO)"
                "status"       "Paid, Open, Overdue, Voided"}}
     "qb_customers"
     {:desc "QuickBooks customers"
      :columns {"id"           "QuickBooks customer ID"
                "display_name" "Customer display name"
                "company_name" "Company name"
                "email"        "Primary email"
                "balance"      "Current balance owed (dollars)"
                "active"       "Whether customer is active"}}
     "qb_payments"
     {:desc "QuickBooks payments — money received"
      :columns {"id"           "Payment ID"
                "customer_id"  "FK to qb_customers.id"
                "total_amount" "Payment amount (dollars)"
                "txn_date"     "Transaction date (ISO)"
                "payment_method" "Check, Credit Card, ACH, etc."}}}
    :tips ["QuickBooks amounts are in dollars (not cents)"
           "balance on customers is the current outstanding amount"]}

   "ga4"
   {:source "Google Analytics 4"
    :marker-tables #{"ga4_events" "ga4_sessions" "analytics_events"
                     "events_intraday" "ga_sessions"}
    :tables
    {"ga4_events"
     {:desc "GA4 events — one row per user interaction event"
      :columns {"event_name"       "Event type (page_view, purchase, scroll, etc.)"
                "event_timestamp"  "Microsecond Unix timestamp"
                "user_pseudo_id"   "GA4 anonymous user ID"
                "event_date"       "YYYYMMDD partition date"
                "geo_country"      "Country from IP geolocation"
                "traffic_source"   "Acquisition source (google, direct, etc.)"
                "device_category"  "desktop, mobile, tablet"
                "event_value_in_usd" "Monetary value if applicable"}}
     "ga4_sessions"
     {:desc "GA4 sessions — aggregated from events per session_id"
      :columns {"session_id"       "GA4 session identifier"
                "user_pseudo_id"   "Anonymous user ID"
                "session_start"    "ISO timestamp of session start"
                "session_duration" "Duration in seconds"
                "page_views"       "Page views in session"
                "events"           "Total events in session"
                "geo_country"      "Country"}}}
    :tips ["GA4 event_timestamp is in microseconds — divide by 1000000 for seconds"
           "event_date is YYYYMMDD string, not ISO date"
           "user_pseudo_id is not a real user ID — it's a cookie-based anonymous identifier"]}

   "jira"
   {:source "Jira"
    :marker-tables #{"jira_issues" "jira_projects" "jira_sprints"
                     "issue" "project" "sprint"}
    :tables
    {"jira_issues"
     {:desc "Jira issues — tickets, bugs, stories, epics"
      :columns {"id"          "Jira issue ID"
                "key"         "Issue key (PROJ-123)"
                "summary"     "Issue title"
                "status"      "Current status (To Do, In Progress, Done, etc.)"
                "issue_type"  "Bug, Story, Task, Epic, Sub-task"
                "priority"    "Highest, High, Medium, Low, Lowest"
                "assignee_id" "FK to user assigned"
                "reporter_id" "FK to user who created"
                "project_id"  "FK to jira_projects.id"
                "created"     "ISO timestamp"
                "updated"     "ISO timestamp of last update"
                "resolution_date" "ISO timestamp when resolved (null if open)"
                "story_points" "Story point estimate"
                "sprint_id"   "FK to jira_sprints.id"}}
     "jira_projects"
     {:desc "Jira projects — containers for issues"
      :columns {"id"   "Project ID"
                "key"  "Project key (e.g. PROJ)"
                "name" "Project name"}}
     "jira_sprints"
     {:desc "Jira sprints — time-boxed iterations"
      :columns {"id"         "Sprint ID"
                "name"       "Sprint name"
                "state"      "active, closed, future"
                "start_date" "ISO timestamp"
                "end_date"   "ISO timestamp"
                "complete_date" "ISO timestamp when completed"}}}
    :tips ["Jira status is a label string, not an ID"
           "resolution_date IS NULL means the issue is still open"
           "story_points may be null for non-story issue types"]}

   "zendesk"
   {:source "Zendesk"
    :marker-tables #{"zendesk_tickets" "zendesk_users" "zendesk_organizations"
                     "ticket" "zendesk__tickets"}
    :tables
    {"zendesk_tickets"
     {:desc "Zendesk support tickets"
      :columns {"id"            "Ticket ID"
                "subject"       "Ticket subject line"
                "status"        "new, open, pending, hold, solved, closed"
                "priority"      "urgent, high, normal, low"
                "type"          "question, incident, problem, task"
                "assignee_id"   "FK to agent user"
                "requester_id"  "FK to requester user"
                "organization_id" "FK to zendesk_organizations.id"
                "created_at"    "ISO timestamp"
                "updated_at"    "ISO timestamp"
                "solved_at"     "ISO timestamp when solved"
                "satisfaction_rating" "good, bad, offered, unoffered"}}}
    :tips ["Zendesk status 'closed' means archived — 'solved' is the meaningful resolution"
           "satisfaction_rating 'offered' means survey sent but not yet answered"]}})

;; ─────────────────────────────────────────────────────────────────────
;; Detection
;; ─────────────────────────────────────────────────────────────────────

(defn- normalize-table-name
  "Normalize a table name for fuzzy matching: lowercase, strip common prefixes."
  [tbl]
  (-> (str/lower-case (str tbl))
      (str/replace #"^(fivetran_|stitch_|airbyte_|raw_)" "")))

(defn detect-sources
  "Given a set of table names, return a vec of detected SaaS sources.
   Each entry: {:source \"Stripe\" :key \"stripe\" :matched-tables [...] :confidence 0-1}"
  [table-names]
  (let [normalized (set (map normalize-table-name table-names))
        raw-set   (set (map str/lower-case (map str table-names)))]
    (->> catalog
         (keep (fn [[source-key {:keys [source marker-tables]}]]
                 (let [markers (map str/lower-case marker-tables)
                       matched (filter (fn [m]
                                         (or (contains? raw-set m)
                                             (contains? normalized m)))
                                       markers)
                       match-count (count matched)]
                   (when (>= match-count 2)
                     {:source     source
                      :key        source-key
                      :matched-tables (vec matched)
                      :confidence (min 1.0 (/ (double match-count) 3.0))}))))
         (sort-by :confidence >)
         vec)))

(defn curated-context-for-table
  "Given a source key and table name, return curated descriptions if available.
   Returns {:table-desc \"...\" :columns {\"col\" \"desc\"} :tips [...]} or nil."
  [source-key table-name]
  (when-let [source-data (get catalog source-key)]
    (let [tbl-lower (str/lower-case (str table-name))
          ;; Try exact match first, then normalized
          entry (or (get (:tables source-data) table-name)
                    (get (:tables source-data) tbl-lower)
                    ;; Try stripping prefix: "stripe_charges" -> look up "stripe_charges"
                    ;; Already direct. Try the Fivetran short name
                    (some (fn [[k v]]
                            (when (= (str/lower-case k) tbl-lower) v))
                          (:tables source-data)))]
      (when entry
        {:table-desc (:desc entry)
         :columns    (:columns entry)
         :tips       (:tips source-data)}))))

(defn curated-descriptions-for-registry
  "Given detected sources and a registry, return a map of
   {table-name {:table-desc ... :columns {...}}} for all recognized tables."
  [detected-sources registry]
  (reduce
   (fn [acc [tbl _cols]]
     (let [match (->> detected-sources
                      (some (fn [{:keys [key]}]
                              (curated-context-for-table key tbl))))]
       (if match
         (assoc acc tbl match)
         acc)))
   {}
   registry))
