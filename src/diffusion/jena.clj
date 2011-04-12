(ns diffusion.jena
    (:require [plaza.rdf.core :as prc]
              plaza.rdf.implementations.jena)
    (:import (com.hp.hpl.jena.query Dataset DatasetFactory DataSource QueryExecutionFactory Syntax)
             (plaza.rdf.implementations.jena JenaModel)))

; Functions copied from plaza.rdf.implementations.jena
    
(defn- process-query-result
  "Transforms a query result into a dicitionary of bindings"
  ([model result]
     (let [vars (iterator-seq (.varNames result))]
       (reduce (fn [acum item] 
           (assoc acum 
                  (keyword (str "?" item)) 
                  (plaza.rdf.implementations.jena/parse-jena-object model (.get result item)))) {} vars))))

(defn direct-query
  "Queries a model and returns a map of bindings"
  ([model-or-dataset query-string]
     (prc/model-critical-read model-or-dataset
          (let [qexec (QueryExecutionFactory/create query-string Syntax/syntaxSPARQL_11 (prc/to-java model-or-dataset))
                results (iterator-seq (.execSelect qexec))]
            (map #(process-query-result model-or-dataset %1) results)))))
            
(defprotocol RDFDataset
    "Operations for accessing models in a named-graph"
    (contains-named-model [dataset uri])
    (default-model [dataset])
    (named-model [dataset uri])
    (names [dataset]))
    
(defprotocol RDFDataSource
    "Operations for modifying models in a named-graph"
    (add-named-model [datasource uri model])
    (remove-named-model [datasource uri])
    (replace-named-model [datasource uri model])
    (set-default-model [datasource model]))
    
(defprotocol CriticalOperations
    (critical-read [datasource f]))

(deftype JenaDataSource [ds] RDFDataset RDFDataSource CriticalOperations prc/JavaObjectWrapper
    (prc/to-java [datasource] ds)
    (contains-named-model [dataset uri] (.containsNamedModel ds uri))
    (default-model [dataset] (JenaModel. (.getDefaultModel ds)))
    (named-model [dataset uri] (JenaModel. (.getNamedModel ds uri)))
    (names [dataset] (iterator-seq (.listNames ds)))
    (add-named-model [datasource uri model] (.addNamedModel ds uri (prc/to-java model)) datasource)
    (remove-named-model [datasource uri] (.removeNamedModel ds uri) datasource)
    (replace-named-model [datasource uri model] (.replaceNamedModel ds uri (prc/to-java model)) datasource)
    (set-default-model [datasource model] (.setDefaultModel ds (prc/to-java model)) datasource)
    (critical-read [datasource f]
                   (do
                     (.enterCriticalSection (.getLock ds))
                     (let [res (f)]
                       (.leaveCriticalSection (.getLock ds))
                       res))))
    
(defn build-datasource []
    (JenaDataSource. (DatasetFactory/create)))
    
