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

(ns mau.datapoint-store.connection-pool
  (:require [clojure.core.async :as asy]
            [clojure.tools.logging :as log])
  (:import [java.lang AutoCloseable]))

(defn return-or-close
  [chan con]
  (if-not (asy/offer! chan con)
    (do (.close con)
        (log/info "Connection closed"))
    (log/info "Connection returned")))

(defn close-all-connections
  [{:keys [base-connection channel]}]
  (asy/close! channel)
  (.close base-connection)
  (loop [con (asy/poll! channel)]
    (if (some? con)
      (do
        (try
          (.close con)
          (catch Exception e))
        (recur (asy/poll! channel))))))
    
(deftype PoolConnection [chan con]
  AutoCloseable
  (close [_]
    (try
      (if (and (not (.isClosed con))
               (.isValid con 0))
        (return-or-close chan con))
      (catch Exception e
        (log/error "Exception " e)))))

(defn get-pool-connection
  "Should be used with with-open and not be closed manually."
  [{:keys [base-connection channel]}]
  (if-let [con (asy/poll! channel)]
    (do (log/info "Reuse connection")
        (PoolConnection. channel con))
    (do (log/info "New connection")
        (PoolConnection. channel (.duplicate base-connection)))))

(defn get-connection
  [pcon]
  (.-con pcon))

(defn create-pool
  "Takes a base-connection that is only used to duplicate new connections. Therefore it must not be
  closed while the pool is used.
  The pool-size is the minimum number of connections that are kept open. There is no maximum of
  parallel connections!"
  [base-con pool-size]
  {:channel (asy/chan pool-size)
   :base-connection base-con})

(comment
  (import java.sql.DriverManager)
  (def pc {:channel (asy/chan 3)
           :base-connection (DriverManager/getConnection "jdbc:duckdb:")})
  
  (dotimes [x 20]
  (future
  (with-open [con (get-connection pc)]
    (println (.isValid (.-con con) 2))
    (Thread/sleep 1000)))
  (Thread/sleep 200))
)
