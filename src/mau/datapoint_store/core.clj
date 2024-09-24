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

(ns mau.datapoint-store.core
  (:require [ring.adapter.jetty :as jetty]
            [ring.middleware.reload :as reload]
            [clojure.core.async :as asy]
            [clojure.tools.logging :as log]
            [mau.datapoint-store.configuration :as conf]
            [mau.datapoint-store.db-api :as db]
            [mau.datapoint-store.rest-api :as rapi]))

(def continue-http (atom true))
(def continue-datastore (atom true))

; Shutdown hook fn
(defn shutdown
  []
  (compare-and-set! continue-http true false)
  (compare-and-set! continue-datastore true false)
  (Thread/sleep 1000)
  (rapi/close-db-pool))

(defn run-http-server
  [srv-cnf]
  (jetty/run-jetty #'rapi/rest-app srv-cnf))

(defn run-dev-http-server
  [srv-cnf]
  (jetty/run-jetty (reload/wrap-reload #'rapi/rest-app) srv-cnf))

(defn startup-http
  [port db-con]
  (log/info "Set DB connection for rest api")
  (rapi/set-db-pool db-con)
  (log/info "Start Jetty on port: " port)
  (let [http-srv (run-http-server {:port port :join? false})]
    (log/info "Jetty started")
    (while @continue-http
      (Thread/sleep 1000))
    (log/info "Stop Jetty")
    (.stop http-srv)))

(defn startup-datastore
  []
  (let [ds-config conf/datastore-config
        http-config conf/http-server-config
        buffer-size (:buffer-size ds-config 10000)
        db-file (:database-file-path ds-config "/database/DatapointStore")
        db-con (db/create-db-connection db-file)
        int-chan (asy/chan (asy/sliding-buffer buffer-size))
        float-chan (asy/chan (asy/sliding-buffer buffer-size))
        bit-chan (asy/chan (asy/sliding-buffer buffer-size))
        string-chan (asy/chan (asy/sliding-buffer buffer-size))]
    (log/info "DB path: " db-file)
    (log/info "Start threads")
    (log/info int-chan)
    (rapi/set-channels int-chan float-chan bit-chan string-chan)
    (future (startup-http (:port http-config) db-con))
    (future (db/value-persister db-con int-chan float-chan bit-chan string-chan continue-datastore)))) 

(defn -main
  [& args]
  (log/info "Starting ...")
  (startup-datastore)

  ; Add shutdown hook
  (.addShutdownHook (Runtime/getRuntime) 
                    (Thread. ^Runnable shutdown))

  (log/info "Main thread start spinning ...")
  (while true
    (Thread/sleep 10000)))

(comment
  (future (run-dev-http-server {:port 6060 :join? true}))
  (startup-datastore)

  (compare-and-set! continue-http false true)
  (compare-and-set! continue-datastore false true)
  (shutdown)
)


