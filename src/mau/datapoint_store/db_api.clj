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

(ns mau.datapoint-store.db-api
  (:require [clojure.core.async :as asy]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc])
  (:import [java.time LocalDateTime Instant ZoneId]
           [java.sql DriverManager]
           [org.duckdb DuckDBAppender]
           [java.nio.file Paths Files]
           [java.nio.file.attribute FileAttribute]))

(defn create-db-connection
  [db-file]
  (let [path-str (str (System/getProperty "user.dir") db-file)
        db-folder (.getParent (Paths/get path-str (into-array String [""])))]
    (Files/createDirectories db-folder (into-array FileAttribute []))
    (DriverManager/getConnection (str "jdbc:duckdb:"  path-str))))

(defn process-tables
  [res]
  (loop [tbl-map {:int false :float false :string false :bit false}]
    (if (.next res)
      (condp = (.getString res 1)
        "IntDatapointStore" (recur (assoc tbl-map :int true))
        "FloatDatapointStore" (recur (assoc tbl-map :float true))
        "BitDatapointStore" (recur (assoc tbl-map :bit true))
        "StringDatapointStore" (recur (assoc tbl-map :string true))
        (recur tbl-map))
      tbl-map)))

(defn check-db-tables
  [db-con]
  (with-open [stmt (.createStatement db-con)
              res (.executeQuery stmt "SELECT table_name FROM information_schema.tables")]
    (empty? (drop-while second (process-tables res))))) 

(defn create-tables
  [db-con]
  (with-open [stmt (.createStatement db-con)]
    (try
    (.execute stmt 
      "CREATE TABLE IntDatapointStore (DatapointId INT4 NOT NULL
                                        , DatapointTimestamp TIMESTAMP NOT NULL
                                        , DatapointValue INT8 NOT NULL)")
      (catch Exception e))
    (try
    (.execute stmt 
      "CREATE TABLE FloatDatapointStore (DatapointId INT4 NOT NULL
                                        , DatapointTimestamp TIMESTAMP NOT NULL
                                        , DatapointValue  FLOAT8 NOT NULL)")
      (catch Exception e))
    (try
    (.execute stmt 
      "CREATE TABLE BitDatapointStore (DatapointId INT4 NOT NULL
                                        , DatapointTimestamp TIMESTAMP NOT NULL
                                        , DatapointValue INT1 NOT NULL)")
      (catch Exception e))
    (try
    (.execute stmt 
      "CREATE TABLE StringDatapointStore (DatapointId INT4 NOT NULL
                                        , DatapointTimestamp TIMESTAMP NOT NULL
                                        , DatapointValue STRING NOT NULL)")
      (catch Exception e))))

(defn check-and-create-db
  [db-con]
  (if (not (check-db-tables db-con))
    (create-tables db-con)))

(defn append
  [db-con chan table]
  (with-open [appender (DuckDBAppender. db-con "main" table)]
    (loop [row-values (asy/poll! chan)]
      (if (some? row-values)
        (do
          (log/debug row-values)
          (let [{:keys [value timestamp value-id]} row-values]
            (.beginRow appender)
            (.append appender value-id)
            (.appendLocalDateTime appender (LocalDateTime/ofInstant (Instant/ofEpochMilli timestamp) (ZoneId/systemDefault)))
            (.append appender value)
            (.endRow appender))
        (recur (asy/poll! chan)))))))

(defn value-persister
  [db-con int-chan float-chan bit-chan string-chan continue?]
  (check-and-create-db db-con)
  (while @continue?
    ; (log/info "Next round of persistence")
    (append db-con int-chan "IntDatapointStore")
    (append db-con float-chan "FloatDatapointStore")
    (append db-con bit-chan "BitDatapointStore")
    (append db-con string-chan "StringDatapointStore")
    (Thread/sleep 50)))

(defn build-db-select
  [table]
  (str "SELECT  json_group_array(DatapointValue) AS Values_JSON
                ,json_group_array(DatapointTimestamp) AS Timestamps_JSON
           FROM (
               SELECT DatapointValue
                      ,DatapointTimestamp
                  FROM main." table
          " WHERE DatapointId  = ?
              AND DatapointTimestamp > ?
              AND DatapointTimestamp < ?
          ORDER BY DatapointTimestamp
          LIMIT 10000)"))

(defn db->json-str
  [res-vec dtype]
  (log/debug res-vec)
  (let [res (first res-vec)]
  (str "[{\"name\": \"Time\", \"values\":" (:Timestamps_JSON res) ", \"type\": \"time\"},"
       "{\"name\": \"Value\", \"values\":"(:Values_JSON res) ", \"type\": \"" dtype "\"}]")))

(defn get-resp-type
  [table]
  (case table
    "IntDatapointStore" "number"
    "FloadDatapointStore" "number"
    "BitDatapointStore" "number"
    "StringDatapointStore" "text"))


(defn read-datapoints
  [db-con table id ts-from ts-to]
  (try
    (log/info "Reading from " table)
    (log/info (build-db-select table)) 
    (with-open [query (jdbc/prepare db-con [(build-db-select table) id ts-from ts-to])]
      (db->json-str (jdbc/execute! query) (get-resp-type table)))
      (catch Exception e
        (log/error (.getMessage e)))))

(comment
  (def test-data-int [{:value 1324, :timestamp 1689871876545, :value-id 957}])
  (def test-data-float [{:value 13.3423, :timestamp 1689871876545, :value-id 2957}])
  (def test-data-bit [{:value true, :timestamp 1689871876545, :value-id 3957}])
  (def test-data-string [{:value "this is a string", :timestamp 1689871876545, :value-id 4957}])

  (let [continue? (atom true)
        int-chan (asy/chan (asy/sliding-buffer 10000))
        float-chan (asy/chan (asy/sliding-buffer 10000))
        bit-chan (asy/chan (asy/sliding-buffer 10000))
        string-chan (asy/chan (asy/sliding-buffer 10000))
        f (future (value-persister int-chan float-chan bit-chan string-chan continue?))]
      (doseq [v test-data-int] (asy/put! int-chan v))
      (doseq [v test-data-float] (asy/put! float-chan v))
      (doseq [v test-data-bit] (asy/put! bit-chan v))
      (doseq [v test-data-string] (asy/put! string-chan v))
      (Thread/sleep 10000)
      (compare-and-set! continue? true false)
      (Thread/sleep 2000)
      (compare-and-set! continue? false true))
)
