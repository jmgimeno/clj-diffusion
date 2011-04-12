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

(deftype JenaDataSource [ds] RDFDataset RDFDataSource prc/RDFModel prc/JavaObjectWrapper
    ; RDFDataset
    (contains-named-model [dataset uri] (.containsNamedModel ds uri))
    (default-model [dataset] (JenaModel. (.getDefaultModel ds)))
    (named-model [dataset uri] (JenaModel. (.getNamedModel ds uri)))
    (names [dataset] (iterator-seq (.listNames ds)))
    ; RDFDataSource
    (add-named-model [datasource uri model] (.addNamedModel ds uri (prc/to-java model)) datasource)
    (remove-named-model [datasource uri] (.removeNamedModel ds uri) datasource)
    (replace-named-model [datasource uri model] (.replaceNamedModel ds uri (prc/to-java model)) datasource)
    (set-default-model [datasource model] (.setDefaultModel ds (prc/to-java model)) datasource)
    ; prc/RDFModel
    (create-resource [datasource ns local] (prc/create-resource (default-model datasource) ns local)) 
    (create-resource [datasource uri] (prc/create-resource (default-model datasource) uri))
    (create-property [datasource ns local] (prc/create-property (default-model datasource) ns local))
    (create-property [datasource uri] (prc/create-property (default-model datasource) uri))
    (create-blank-node [datasource]  (prc/create-blank-node (default-model datasource)))
    (create-blank-node [datasource id] (prc/create-blank-node (default-model datasource) id))
    (create-literal [datasource lit] (prc/create-literal (default-model datasource) lit))
    (create-literal [datasource lit lang] (prc/create-literal (default-model datasource) lit lang))
    (create-typed-literal [datasource lit] (prc/create-typed-literal (default-model datasource) lit))
    (create-typed-literal [datasource lit type](prc/create-typed-literal (default-model datasource) lit type))
    (critical-write [datasource f] (prc/critical-write (default-model datasource) f))
    (critical-read [datasource f] (prc/critical-read (default-model datasource) f))
    (add-triples [datasource triples] (prc/add-triples (default-model datasource) triples))
    (remove-triples [datasource triples] (prc/remove-triples (default-model datasource) triples))
    (walk-triples [datasource f] (prc/walk-triples (default-model datasource) f))
    (load-stream [datasource stream format] (prc/load-stream (default-model datasource) stream format))
    (output-string  [datasource format] (prc/output-string (default-model datasource) format))
    (output-string  [datasource writer format] (prc/output-string (default-model datasource) writer format))
    (query [datasource query] (prc/query (default-model datasource) query))
    (query-triples [datasource query] (prc/query-triples (default-model datasource) query))
    ; prc/JavaObjectWrapper
    (prc/to-java [datasource] ds))

    
(defn build-datasource []
    (JenaDataSource. (DatasetFactory/create)))
    
