
Based on 
https://vidyats.medium.com/how-to-switch-from-airflow-default-sequential-executor-to-local-executor-ad77f2e4b57a

1. git clone label-inspector (in airflow directory)
2. docker compose up
3. https://localhost:8080
   * login: airflow, 
   * pass: airflow
 

 ```mermaid
 flowchart TD
    n31["interesting-score.rocks\n11h52m\n160MB"]
    n29["allowed-categories.txt\n31s\n18MB"]
    n14["allowed-lists.txt\n5s\n835KB"]
    n30["categories.json\n31s\n55MB"]
    n24["category_members.jsonl\n1m31s\n702MB"]
    n33["category_members_all_info.jsonl\n24m\n2.8GB"]
    n17["db1.rocks\n5m\n182MB"]
    n16["db1_rev.rocks\n5m\n136MB"]
    n19["db3.rocks\n2h26m\n18MB"]
    n28["enwiki-latest-categorylinks.sql.gz\n12m\n3.4GB"]
    n13["enwiki-latest-pagelinks.sql.gz\n27m\n7.5GB"]
    n27["enwiki-categories.csv\n48m\n1.3GB"]
    n21["enwiki-latest-redirect.sql.gz\n34s\n153MB"]
    n12["enwiki-pagelinks.csv\n4h28m\n936MB"]
    n20["index_enwiki-latest.db\n19m\n2.2GB"]
    style n20 fill:#9f9
    n9["list_members.jsonl\n1m31s\n765MB"]
    n7["list_members_all_info.jsonl\n7m\n750MB"]
    n15["lists.json\n7s\n12MB"]
    n26["mapped-categories.csv\n8m\n1.4GB"]
    n11["mapped-lists.csv\n1m49s\n1.8GB"]
    n6["merged.jsonl\n4m\n3.4GB"]
    style n6 fill:#9f9
    n5["merged-without-letters.jsonl\n3m40s\n3.4GB"]
    n4["merged-without-duplicates.jsonl\n3m30s\n3.4GB"]
    n3["merged_final.jsonl\n32m\n8.2GB"]
    style n3 fill:#9f9
    n25["sorted-categories.csv\n41s\n1.4GB"]
    n10["sorted-lists.csv\n24s\n1.8GB"]
    n32["suggestable_domains.csv\n110MB"]
    n23["validated_category_members.jsonl\n37m\n735MB"]
    style n23 fill:#9f9
    n8["validated_list_members.jsonl\n32m16s\n199MB"]
    style n8 fill:#9f9
    n34["latest-truthy.nt.bz2\n2h10m\n36GB"]
    n35["latest-truthy.nt.bz2.filtered.bz2\n2h40m\n3.3GB"]
    style n35 fill:#9f9
    n37["enwiki-latest-page_props.sql.gz\n2m\n357MB"]
    n38["enwiki-latest-page.sql.gz\n8m\n357MB"]
    n39["qrank.csv\n6s\n332MB"]
    n40["db2.rocks\n2h26m\n630MB"]
    n41["db4.rocks\n2h26m\n2.7MB"]
    n42["db5.rocks\n2h26m\n7.3GB"]
    n43["db6.rocks\n2h26m\n70MB"]
    n44["avatars-emojis.csv\n9.4KB"]
    n45["uniq_category_members.txt\n33m\n57MB"]
    n46["uniq_list_members.txt\n23m\n34MB"]
    n31 --> n33
    n31 --> n7
    n8 --> n46
    n23 --> n45
    n45 --> n31 
    n46 --> n31 
    n30 --> n29  
    n20 --> n14  
    n15 --> n14  
    n16 --> n30  
    n19 --> n30  
    n30 --> n24  
    n25 --> n24  
    n32 --> n33  
    n23 --> n33  
    n20 --> n17  
    n20 --> n16  
    n29 --> n27  
    n28 --> n27  
    n14 --> n12  
    n13 --> n12  
    n21 --> n20  
    n15 --> n9  
    n10 --> n9  
    n32 --> n7  
    n8 --> n7  
    n16 --> n15  
    n19 --> n15  
    n27 --> n26  
    n20 --> n26  
    n12 --> n11  
    n20 --> n11  
    n33 --> n6  
    n7 --> n6  
    n6 --> n5  
    n5 --> n4  
    n4 --> n3  
    n26 --> n25  
    n11 --> n10  
    n24 --> n23  
    n9 --> n8  
    n34 --> n35
    n38 --> n20
    n17 --> n8  
    n40 --> n8  
    n43 --> n8  
    n20 --> n8  
    n17 --> n23  
    n40 --> n23  
    n43 --> n23  
    n20 --> n23  
    n42 --> n33  
    n42 --> n7  
    n20 --> n33  
    n20 --> n7  
    n41 --> n6 
    n44 --> n3 
    n39 --> n33 
    n39 --> n7 
    n35 --> n19 
    n35 --> n40 
    n35 --> n41 
    n35 --> n42 
    n35 --> n43 
    n37 --> n20
 ```
