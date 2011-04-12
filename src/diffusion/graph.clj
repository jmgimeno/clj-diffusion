(ns diffusion.graph
    (:require [clojure.set :as set] 
              [loom.graph :as lg]))

; Nodes

(defrecord Node [name type])

(defn make-user [user] 
    (Node. user :User))

(defn make-item [item] 
    (Node. item :Item))

(defn make-tag [tag] 
    (Node. tag :Tag))

(defn user? [node]
    (= (:type node) :User))
    
(defn item? [node]
    (= (:type node) :Item))

(defn tag? [node]
    (= (:type node) :Tag))

; Graph creation and access

(defn new-graph []
    (lg/graph))

(defn add-user [g user]
    (lg/add-nodes g (make-user user)))

(defn add-item [g item]
    (lg/add-nodes g (make-item item)))
    
(defn add-tag [g tag]
    (lg/add-nodes g (make-tag tag)))
    
(defn add-review [g user item]
    (let [user-node (make-user user)
          item-node (make-item item)]
          (-> g
              (lg/add-nodes user-node item-node)
              (lg/add-edges [user-node item-node]))))
    
(defn tag-item [g item tag]
    (let [item-node (make-item item)
          tag-node (make-tag tag)]
          (-> g
              (lg/add-nodes item-node tag-node)
              (lg/add-edges [item-node tag-node]))))
              
(defn count-reviews-of-user [g user]
    (count (lg/neighbors g (make-user user))))

(defn- count-filtered-neighbors-of [g node pred?]
    (->> node
         (lg/neighbors g)
         (filter pred?)
         count))

(defn count-reviewers-of-item [g item]
    (count-filtered-neighbors-of g (make-item item) user?))

(defn count-tags-of-item [g item]
    (count-filtered-neighbors-of g (make-item item) tag?))
         
(defn count-items-with-tag [g tag]
    (count-filtered-neighbors-of g (make-tag tag) item?))

(defn all-users [g]
    (map :name (filter user? (lg/nodes g))))
    
(defn all-items [g]
    (map :name (filter item? (lg/nodes g))))
    
(defn all-tags [g]
    (map :name (filter tag? (lg/nodes g))))

(defn- filtered-neighbors-names [g node pred?] 
    (->> node
         (lg/neighbors g)
         (filter pred?)
         (map :name)))
         
(defn reviewed-by [g item]
    (filtered-neighbors-names g (make-item item) user?))
         
(defn reviews-of [g user]
    (filtered-neighbors-names g (make-user user) item?))
    
(defn tagged-with [g tag]
    (filtered-neighbors-names g (make-tag tag) item?))
    
(defn tags-of [g item]
    (filtered-neighbors-names g (make-item item) tag?))
    
(defn new-items-for [g user]
    (set/difference (set (all-items g)) (set (reviews-of g user))))
         
; Diffusion of activations

(defn initial-activations [g user]
    (merge 
        (zipmap (reviews-of g user) (repeat 1.0))
        (zipmap (new-items-for g user) (repeat 0.0))))

(defn- counts-for [g selector-fn count-fn]
    (let [names (selector-fn g)]
        (zipmap names (map (partial count-fn g) names))))
        
(defn counts-users-to-items [g]
    (counts-for g all-users count-reviews-of-user))
            
(defn counts-items-to-users [g]
    (counts-for g all-items count-reviewers-of-item))
            
(defn counts-items-to-tags [g]
    (counts-for g all-items count-tags-of-item))
        
(defn counts-tags-to-items [g]
    (counts-for g all-tags count-items-with-tag))

(defn- merge-activations [activations degree neighbors-fn names]
    (let [activation-from 
            (fn [n] (/ (activations n) (degree n)))
          incoming-activation
            (fn [n] (map activation-from (neighbors-fn n)))
          activation-for-node
            (fn [n] (reduce + (incoming-activation n)))]            
        (reduce 
            (fn [act n] (assoc act n (activation-for-node n)))
            {}
            names)))

(defn diffusion-users-items-for-user [g user]
    (-> (initial-activations g user)
        (merge-activations
            (counts-items-to-users g)
            (partial reviews-of g)
            (all-users g))
        (merge-activations
            (counts-users-to-items g)
            (partial reviewed-by g)
            (new-items-for g user))))
            
(defn diffusions-users-items [g]
    (reduce 
        (fn [diffusions user]
            (assoc diffusions user (diffusion-users-items-for-user g user)))
        {}
        (all-users g)))
        
(defn diffusion-items-tags-for-user [g user]
    (-> (initial-activations g user)
        (merge-activations
            (counts-items-to-tags g)
            (partial tagged-with g)
            (all-tags g))
        (merge-activations
            (counts-tags-to-items g)
            (partial tags-of g)
            (new-items-for g user))))

(defn diffusions-items-tags [g]
    (reduce 
        (fn [diffusions user]
            (assoc diffusions user (diffusion-items-tags-for-user g user)))
        {}
        (all-users g)))
