from cassandra.cluster import Cluster 


new_cluster = Cluster()

session = new_cluster.connect('pinterest') # = keyspace 

session.execute("""CREATE TABLE pinterest_batch_new 
    (
    id_index int PRIMARY KEY,
    title text,
    category text, 
    unique_id text, 
    description text, 
    follower_count text, 
    tag_list text,
    is_image_or_video text, 
    image_src text, 
    downloaded int, 
    save_location text);""")
