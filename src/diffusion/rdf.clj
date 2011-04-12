(ns diffusion.rdf
    (:require [diffusion.jena :as dj]
              [plaza.rdf.core :as prc]
              [plaza.rdf.sparql :as prs]
              plaza.rdf.implementations.jena
              plaza.utils))

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
    (dj/build-datasource))

(defn add-user [datasource user]
    (prc/with-model datasource
        (prc/model-add-triples 
            [[(make-user user) prc/rdf:type [:diffusion :User]]]))
    datasource)

(defn add-item [datasource item]
    (prc/with-model datasource
        (prc/model-add-triples 
            [[(make-item item) prc/rdf:type [:diffusion :Item]]]))
    datasource)
    
(defn add-tag [datasource tag]
    (prc/with-model datasource
        (prc/model-add-triples 
            [[(make-tag tag) prc/rdf:type [:diffusion :Tag]]]))
    datasource)
    
(defn add-review [datasource user item]
    (let [user-resource (make-user user)
          item-resource (make-item item)]
        (add-user datasource user)
        (add-item datasource item)
        (prc/with-model datasource
            (prc/model-add-triples
                [[user-resource [:diffusion :has-reviewed] item-resource]]))
        datasource))

(defn tag-item [datasource item tag]
    (let [item-resource (make-item item)
          tag-resource (make-tag tag)]
        (add-item datasource item)
        (add-tag datasource tag)
        (prc/with-model datasource
            (prc/model-add-triples
                [[item-resource [:diffusion :has-tag] tag-resource]]))
        datasource))
     
(defn- count-relation [datasource {subject :subject property :property object :object}]
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
         (dj/direct-query datasource)
         first
         :?counter
         prc/literal-value))

(defn count-reviews-of-user [datasource user]
    (count-relation datasource {:subject (format "user:%s" user) :property "has-reviewed"}))

(defn count-reviewers-of-item [datasource item]
    (count-relation datasource {:object (format "item:%s" item) :property "has-reviewed"}))

(defn count-tags-of-item [datasource item]
    (count-relation datasource {:subject (format "item:%s" item) :property "has-tag"}))

(defn count-items-with-tag [datasource tag]
    (count-relation datasource {:object (format "tag:%s" tag) :property "has-tag"}))

(defn all-of-type [datasource type]
    (->> (prs/defquery
            (prs/query-set-type :select)
            (prs/query-set-vars [:?elem])
            (prs/query-set-pattern
                (prs/make-pattern [[:?elem prc/rdf:type [:diffusion type]]])))
         (prs/model-query datasource)
         (map :?elem)
         (map prc/resource-qname-local)))
           
(defn count-of-type [datasource type]
    (->> (format "PREFIX diffusion: <%1$s>
                  SELECT (COUNT(?elem) AS ?counter) 
                  WHERE { ?elem a diffusion:%2$s . }" 
                  (prc/find-ns-registry :diffusion)
                  (plaza.utils/keyword-to-string type))
         (dj/direct-query datasource)
         first
         :?counter
         prc/literal-value))
        
(defn all-users [datasource]
    (all-of-type datasource :User))
    
(defn all-items [datasource]
    (all-of-type datasource :Item))
    
(defn all-tags [datasource]
    (all-of-type datasource :Tag))

(defn- get-relations-other [datasource {subject :subject property :property object :object}]
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
         (dj/direct-query datasource)
         (map :?elem)
         (map prc/resource-qname-local)))
             
(defn reviewed-by [datasource item]
    (get-relations-other datasource {:property "has-reviewed" :object (format "item:%s" item)}))

(defn reviews-of [datasource user]
    (get-relations-other datasource {:subject (format "user:%s" user) :property "has-reviewed"}))
         
(defn tagged-with [datasource tag]
    (get-relations-other datasource {:property "has-tag" :object (format "tag:%s" tag)}))

(defn tags-of [datasource item]
    (get-relations-other datasource {:subject (format "item:%s" item) :property "has-tag"}))
    
(defn new-items-for [datasource user]
    (->> (format "PREFIX diffusion: <%1$s>
                  PREFIX user: <%2$s>
                  SELECT ?item
                  WHERE { ?item a diffusion:Item .
                          FILTER NOT EXISTS { user:%3$s diffusion:has-reviewed ?item } }"
                  (prc/find-ns-registry :diffusion)
                  (prc/find-ns-registry :user)
                  user)
         (dj/direct-query datasource)
         (map :?item)
         (map prc/resource-qname-local)))



     