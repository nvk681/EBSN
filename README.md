# EBSN
Event-based social networks (EBSNs), such as Meetup and Plancast, which offer platforms for users to plan, arrange, and publish events, have gained increasing popularity and rapid growth. EBSNs capture not only the online social relationship, but also the offline interactions from offline events. They contain rich heterogeneous information, including multiple types of entities, such as users, events, groups and tags, and their interaction relations. Three recommendation tasks, namely recommending groups to users, recommending tags to groups, and recommending events to users, have been explored in three separate studies. However, none of the proposed methods can handle all the three recommendation tasks. In this paper, we propose a general graph-based model, called HeteRS, to solve the three recommendation problems on EBSNs in one framework. Our method models the rich information with a heterogeneous graph and considers the recommendation problem as a query-dependent node proximity problem. To address the challenging issue of weighting the influences between different types of entities, we propose a learning scheme to set the influence weights between different types of entities. Experimental results on two real-world datasets demonstrate that our proposed method significantly outperforms the state-of-the-art methods for all the three recommendation tasks, and the learned influence weights help understanding user behaviors.
