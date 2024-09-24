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

(ns mau.datapoint-store.configuration
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]))

(def datastore-config-file "./configuration/datastore-configuration.edn")
(def http-server-config-file "./configuration/http-server-configuration.edn")

(defn get-edn-config
  [file]
  (edn/read-string (slurp (io/reader file))))

; Load at startup and throw if not found
(def datastore-config
  (try
    (get-edn-config datastore-config-file)
    (catch Exception e
      (log/error "Datastore configuration not found! " e)
      (throw e)
      )))

(def http-server-config
  (try
    (get-edn-config http-server-config-file)
    (catch Exception e
      (log/error "Http server configuration not found! " e)
      (throw e)
      )))

