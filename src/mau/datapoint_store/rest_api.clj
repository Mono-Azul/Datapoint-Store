;********************************************************************************
; Copyright (c) 2024 Jens Hofer
; 
;  This program and the accompanying materials are made available under the
;  terms of the Eclipse Public License 2.0 which is available at
;  http://www.eclipse.org/legal/epl-2.0.
;
;  This Source Code may also be made available under the following Secondary Licenses
;  when the conditions for such availability set forth in the Eclipse Public License, 
;  v. 2.0 are satisfied: GNU General Public License v3.0 or later 
;
;  SPDX-License-Identifier: EPL-2.0 OR GPL-3.0-or-later
; *******************************************************************************/

(ns mau.datapoint-store.rest-api
  (:require [ring.util.response :as response]
            [ring.middleware.defaults :as rdef]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clojure.core.async :as asy]
            [mau.datapoint-store.db-api :as db]
            [mau.datapoint-store.connection-pool :as pool])
  (:import [java.io PushbackReader]
           [java.time LocalDateTime]))

; These should be set from outside via set-channels
(def int-chan (atom nil))
(def float-chan (atom nil))
(def bit-chan (atom nil))
(def string-chan (atom nil))
(def db-con (atom nil))
(def db-con-pool (atom nil))

(defn set-channels
  [ichan fchan bchan schan]
  (reset! int-chan ichan)
  (reset! float-chan fchan)
  (reset! bit-chan bchan)
  (reset! string-chan schan))

(defn set-db-pool
  [base-con]
  (reset! db-con base-con)
  (reset! db-con-pool (pool/create-pool base-con 5))
  (log/info @db-con-pool))

(defn close-db-pool
  []
  (pool/close-all-connections @db-con-pool))

(defn edn-body2data
  [body]
  (with-open [rdr (io/reader body)
              edn-rdr (PushbackReader. rdr)]
    (edn/read edn-rdr)))

(defn check-insert-data
  [dat]
  (if (and 
        (some? (:value dat))
        (some? (:timestamp dat))
        (some? (:value-id dat)))
    dat
    nil))

(defprotocol ChannelFanout
  "In which channel goes the value"
  (fanout [d x]))

(extend-type java.lang.Long
  ChannelFanout
  (fanout [d x]
    (asy/put! @int-chan x)))

(extend-type java.lang.Double
  ChannelFanout
  (fanout [d x]
    (asy/put! @float-chan x)))

(extend-type java.math.BigDecimal
  ChannelFanout
  (fanout [d x]
    (asy/put! @int-chan x)))

(extend-type java.lang.Boolean
  ChannelFanout
  (fanout [d x]
    (asy/put! @bit-chan x)))

(extend-type java.lang.String
  ChannelFanout
  (fanout [d x]
    (asy/put! @string-chan x)))

(extend-type java.lang.Object
  ChannelFanout
  (fanout [d x]
    (log/error "Fan-out datatype mismatch: " x (type (:value x)))))

(defn datatyp-fanout
  [dat]
  (condp instance? (:value dat)
   java.lang.Long (asy/put! @int-chan dat)
   java.lang.Double (asy/put! @float-chan dat)
   java.math.BigDecimal (asy/put! @int-chan dat)
   java.lang.Boolean (asy/put! @bit-chan dat)
   java.lang.String (asy/put! @string-chan dat)
   (log/error "Fan-out datatype mismatch: " dat (type (:value dat)))))


(defn process-insert-datapoints
  [request]
  (log/debug request)
  (try
    (let [data (edn-body2data (:body request))
          data-checked (reduce #(conj %1 (check-insert-data %2)) [] (flatten data))]
      (log/debug data-checked)
      (doseq [dat data-checked] (fanout (:value dat) dat)))
      ; (doseq [dat data-checked] (datatyp-fanout dat)))
    {:status 200 :body "ok"}
    (catch Exception e
      {:status 300 :body "Error"})))

(defn check-localdatetime
  [ts-str]
  (try
    (LocalDateTime/parse ts-str)
    (catch Exception e
      nil)))

(defn check-int
  [int-str]
  (try
    (Integer/parseInt int-str)
    (catch Exception e
      nil)))

(defn check-parameters
  [id ts-from ts-to]
  (cond
    (nil? (check-int id)) {:status 400 :body "Datapoints have integer IDs"}
    (nil? (check-localdatetime ts-from)) {:status 400 :body "From timestamp not parsable"}
    (nil? (check-localdatetime ts-to)) {:status 400 :body "To timestamp not parsable"}))

(defn return-datapoints
  [dtype id ts-from ts-to]
  (if-let [ret-msg (check-parameters id ts-from ts-to)]
    ret-msg
    (with-open [db-pcon (pool/get-pool-connection @db-con-pool)]
      (let [ret-body {:status 200 :headers {"Content-Type" "application/json"}}
            db-con (pool/get-connection db-pcon)]
        (log/info ret-body)
        (condp = dtype
          "int" (assoc ret-body :body (db/read-datapoints db-con "IntDatapointStore" id ts-from ts-to)) 
          "float" (assoc ret-body :body (db/read-datapoints db-con "FloatDatapointStore" id ts-from ts-to)) 
          "bit" (assoc ret-body :body (db/read-datapoints db-con "BitDatapointStore" id ts-from ts-to))
          "string" (assoc ret-body :body (db/read-datapoints db-con "StringDatapointStore" id ts-from ts-to)) 
          {:status 300 :body "Error"})))))

(defroutes compojure-routes
  (GET "/test" [& params] {:status 200 :body "Test successful"})
  (POST "/insert-datapoints" request (process-insert-datapoints request))
  (GET "/datapoint/:dtype/:id" [dtype id ts-from ts-to] (return-datapoints dtype id ts-from ts-to))
  (GET "/" [] (response/redirect "test"))
  (route/not-found {:status 404 :body "Resource not found."}))

(def rest-app
  (-> compojure-routes
      (rdef/wrap-defaults 
      (assoc-in rdef/site-defaults [:security :anti-forgery] false))))

(comment
  (def test-data [{:value 0, :timestamp 1689871876545, :value-id 957} {:value 0, :timestamp 1689871876545, :value-id 1001} {:value 0, :timestamp 1689871876545, :value-id 1002} {:value 0, :timestamp 1689871876545, :value-id 2} {:value 0, :timestamp 1689871876545, :value-id 3222654} {:value 0, :timestamp 1689871876545, :value-id 19845} {:value 0, :timestamp 1689871876545, :value-id 31215} {:value 0, :timestamp 1689871876545, :value-id 98} {:value 0, :timestamp 1689871876545, :value-id 88} {:value 0, :timestamp 1689871876545, :value-id 187} {:value 0, :timestamp 1689871876545, :value-id 359} {:value 0, :timestamp 1689871876545, :value-id 18}])

(for [x test-data]
  (datatyp-fanout x))
)

