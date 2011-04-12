(ns diffusion.rdf
    (:use [diffusion.jena :only [direct-query]])
    (:require [plaza.rdf.core :as prc]
              [plaza.rdf.sparql :as psparql]
              plaza.rdf.implementations.jena
              [plaza.utils :as putils]))

(plaza.rdf.implementations.jena/init-jena-framework)

(prc/register-rdf-ns :diffusion "http://rhizomik.net/diffusion#")
(prc/register-rdf-ns :user "http://rhizomik.net/diffusion/user/")
(prc/register-rdf-ns :item "http://rhizomik.net/diffusion/item/")
(prc/register-rdf-ns :tag "http://rhizomik.net/diffusion/tag/")

(defn make-user [user]
    (prc/rdf-resource :user user))
    
(defn make-item [item]
    (prc/rdf-resource :item item))
    
(defn make-tag [tag]
    (prc/rdf-resource :tag tag))

(defn user? [resource]
    (= (prc/resource-qname-prefix resource)
       (prc/find-ns-registry :user)))
    
(defn item? [resource]
    (= (prc/resource-qname-prefix resource)
       (prc/find-ns-registry :item)))
    
(defn tag? [resource]
    (= (prc/resource-qname-prefix resource)
       (prc/find-ns-registry :tag)))
    
(defn new-graph []
    (prc/build-model))

(defn add-user [model user]
    (prc/with-model model
        (prc/model-add-triples 
            [[(make-user user) prc/rdf:type [:diffusion :User]]]))
    model)

(defn add-item [model item]
    (prc/with-model model
        (prc/model-add-triples 
            [[(make-item item) prc/rdf:type [:diffusion :Item]]]))
    model)
    
(defn add-tag [model tag]
    (prc/with-model model
        (prc/model-add-triples 
            [[(make-tag tag) prc/rdf:type [:diffusion :Tag]]]))
    model)
    
(defn add-review [model user item]
    (let [user-resource (make-user user)
          item-resource (make-item item)]
        (prc/with-model model
            (add-user model user)
            (add-item model item)
            (prc/model-add-triples
                [[user-resource [:diffusion :has-reviewed] item-resource]]))
        model))

(defn tag-item [model item tag]
    (let [item-resource (make-item item)
          tag-resource (make-tag tag)]
        (prc/with-model model
            (add-item model item)
            (add-tag model tag)
            (prc/model-add-triples
                [[item-resource [:diffusion :has-tag] tag-resource]]))
        model))
     
(defn- count-relation [model {subject :subject property :property object :object}]
    (->> (format "PREFIX diffusion: <%1$s> 
                  PREFIX user: <%2$s>
                  PREFIX item: <%3$s>
                  PREFIX tag: <%4$s>
                  SELECT (COUNT(?elem) AS ?counter)
                  WHERE { %5$s diffusion:%6$s %7$s . }"
                  (prc/find-ns-registry :diffusion)
                  (prc/find-ns-registry :user)
                  (prc/find-ns-registry :item)
                  (prc/find-ns-registry :tag)
                  (if subject subject "?elem")
                  property
                  (if object object "?elem"))
         (direct-query model)
         first
         :?counter
         prc/literal-value))

(defn count-reviews-of-user [model user]
    (count-relation model {:subject (format "user:%s" user) :property "has-reviewed"}))

(defn count-reviewers-of-item [model item]
    (count-relation model {:object (format "item:%s" item) :property "has-reviewed"}))

(defn count-tags-of-item [model item]
    (count-relation model {:subject (format "item:%s" item) :property "has-tag"}))

(defn count-items-with-tag [model tag]
    (count-relation model {:object (format "tag:%s" tag) :property "has-tag"}))

(defn all-of-type [model type]
    (->> (psparql/defquery
            (psparql/query-set-type :select)
            (psparql/query-set-vars [:?elem])
            (psparql/query-set-pattern
                (psparql/make-pattern [[:?elem prc/rdf:type [:diffusion type]]])))
         (psparql/model-query model)
         (map :?elem)
         (map prc/resource-qname-local)))
           
(defn count-of-type [model type]
    (->> (format "PREFIX diffusion: <%1$s>
                  SELECT (COUNT(?elem) AS ?counter) 
                  WHERE { ?elem a diffusion:%2$s . }" 
                  (prc/find-ns-registry :diffusion)
                  (putils/keyword-to-string type))
         (direct-query model)
         first
         :?counter
         prc/literal-value))
        
(defn all-users [model]
    (all-of-type model :User))
    
(defn all-items [model]
    (all-of-type model :Item))
    
(defn all-tags [model]
    (all-of-type model :Tag))

(defn- get-relations-other [model {subject :subject property :property object :object}]
    (->> (format "PREFIX diffusion: <%1$s> 
                  PREFIX user: <%2$s>
                  PREFIX item: <%3$s>
                  PREFIX tag: <%4$s>
                  SELECT ?elem
                  WHERE { %5$s diffusion:%6$s %7$s . }"
                  (prc/find-ns-registry :diffusion)
                  (prc/find-ns-registry :user)
                  (prc/find-ns-registry :item)
                  (prc/find-ns-registry :tag)
                  (if subject subject "?elem")
                  property
                  (if object object "?elem"))
         (direct-query model)
         (map :?elem)
         (map prc/resource-qname-local)))
             
(defn reviewed-by [model item]
    (get-relations-other model {:property "has-reviewed" :object (format "item:%s" item)}))

(defn reviews-of [model user]
    (get-relations-other model {:subject (format "user:%s" user) :property "has-reviewed"}))
         
(defn tagged-with [model tag]
    (get-relations-other model {:property "has-tag" :object (format "tag:%s" tag)}))

(defn tags-of [model item]
    (get-relations-other model {:subject (format "item:%s" item) :property "has-tag"}))
    
(defn new-items-for [model user]
    (->> (format "PREFIX diffusion: <%1$s>
                  PREFIX user: <%2$s>
                  SELECT ?item
                  WHERE { ?item a diffusion:Item .
                          FILTER NOT EXISTS { user:%3$s diffusion:has-reviewed ?item } }"
                  (prc/find-ns-registry :diffusion)
                  (prc/find-ns-registry :user)
                  user)
         (direct-query model)
         (map :?item)
         (map prc/resource-qname-local)))



     