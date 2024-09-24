;****************************************************************************** 
; This file is free software: you can redistribute it and/or modify it under 
; the terms of the GNU General Public License as published by the Free Software
; Foundation, either version 3 of the License, or (at your option) any later 
; version.
;
; This program is distributed in the hope that it will be useful, but WITHOUT 
; ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
; FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
;
; See <https://www.gnu.org/licenses/>.
;
; Copyright (c) 2024 Jens Hofer
;
; SPDX-License-Identifier: GPL-3.0-or-later
;******************************************************************************

(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:import (java.io File)))

(def build-folder "run-folder")
(def main-ns "mau.datapoint-store.core")
(def build-aliaes [:jopts])

(def start-script-template
  (str "#!/usr/bin/env bash\n"
       "SOURCE=\"${BASH_SOURCE[0]}\"\n"
       "while [ -h \"$SOURCE\" ];\n"
       "do\n"
       "    DIR=\"$( cd -P \"$( dirname \"$SOURCE\" )\" && pwd )\"\n"
       "    SOURCE=\"$(readlink \"$SOURCE\")\" [[ $SOURCE != /* ]] && SOURCE=\"$DIR/$SOURCE\"\n"
       "done\n"
       "APP_HOME=\"$( cd -P \"$( dirname \"$SOURCE\" )\" && pwd )\"\n"
       "cd $APP_HOME\n"))

(defn clean [_]
  (b/delete {:path build-folder}))

(defn clean-logging [_]
  (b/delete {:path "logging/logs"}))

(defn- build-indy
  "Builds an independent copy of the project. Independent means all jars are included, but the project's source
  is not compiled, just copied. In addition a start script is included, which should be used as the deps.edn
  does have the maven dependencies. Here is what happens:
  1. Make a new output dir from parameter out-dir
  2. Copy all jars from the local repo into the new libs dir.
  3. Copy all dirs from deps.edn paths that got merged into the classpath
  4. Copy deps.edn
  5. Add start script which will call clojure via deps.edn but with a replaced classpath to include the jars
  from the libs dir
  6. The aliases parameter takes the deps.edn aliases as a coll"
  [main-ns aliases out-dir]
  (let [libs (io/file (str out-dir "/libs"))
        basis (b/create-basis {:aliases aliases})
        basis-cp (:classpath basis)
        basis-paths (concat (:paths basis) (get-in basis [:classpath-args :extra-paths]))
        cp-file-paths (reduce #(if (some? (:lib-name (val %2))) (conj %1 (key %2)) %1) [] basis-cp)
        src-dir-paths (reduce #(if (some? (:path-key (val %2))) (conj %1 (key %2)) %1) [] basis-cp)
        start-script (io/file (str out-dir "/start-script"))
        java-opts (get-in basis [:jopts :jvm-opts])
        java-cmd (b/java-command {:java-cmd "java"
                                   :cp (into ["libs" "libs/*"] basis-paths)
                                   :basis {}
                                   :main (str "clojure.main -m " main-ns)
                                   :java-opts (conj java-opts "-XX:-OmitStackTraceInFastThrow")})]
    (if (.exists (io/file out-dir))
      (do (println (str out-dir " exists already")) 
          (System/exit 1)))

    ; Copy jars
    (doseq [cp-elem cp-file-paths]
      (b/copy-file {:src cp-elem
                    :target  (str libs "/" (.getName (io/file cp-elem)))}))
    
    ; Copy path dirs
    (doseq [dir-path src-dir-paths]
      (b/copy-dir {:src-dirs [dir-path] 
                   :target-dir (str out-dir "/" dir-path)}))

    ; Copy deps.edn
    (b/copy-file {:src "deps.edn"
                  :target (str out-dir "/deps.edn")})

    ; Add start-script
    (spit start-script start-script-template)
    (spit start-script (str "exec " (string/join " " (:command-args java-cmd))) :append true)

    ; (spit start-script 
    ;       (str "exec clojure -Scp " 
    ;            (string/join ":" (concat ["libs" "libs/*"] basis-paths))
    ;            " -M" (apply str aliases) " -m " main-ns) 
    ;       :append true)

    (.setExecutable start-script true false)))

(defn create-run-folder
  [_]
  (clean _)
  (clean-logging _)
  (build-indy main-ns build-aliaes build-folder)) 
