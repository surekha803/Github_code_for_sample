import itertools
import numpy as np
import pandas as pd
import os
from pandas import DataFrame
import csv

import csvwriter as csvwriter

project_id = 'github-project-323219'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'github-project-323219-d094772f0be4.json'
sql1 = "select * from github-project-323219.github_analysis_25k.25k_distinct_repo_ids"
distinct_repo = pd.read_gbq(sql1, project_id=project_id, dialect='standard')
print(distinct_repo.head())
distinct_repo_ids=distinct_repo.stack().tolist()
print(distinct_repo_ids)

CC_query = "select * from github-project-323219.github_analysis_25k.CC_for_all_repos"
CC_for_all_repos = pd.read_gbq(CC_query, project_id=project_id, dialect='standard')
print(CC_for_all_repos.head())

from collections import defaultdict
PartB_refrepos = defaultdict(list)
PartB_refrepos_count =[]
i=0
for ids in distinct_repo_ids:
    print("************************loop number******************",i)
    print("""select  distinct actor_id as actor_ids from """+"`"+"github-project-323219.github_analysis_25k.github_dataset_with_part"""+"`"+""" 
                  where  Type='PullRequestEvent' and repo_id="""+str(ids)+""" """)
    sql2 ="select  distinct actor_id as actor_ids from """+"`"+"github-project-323219.github_analysis_25k.github_dataset_with_part"""+"`"+""" 
                  where  Type='PullRequestEvent' and repo_id="""+str(ids)+""" """
    pull_records_on_focal_repo = pd.read_gbq(sql2, project_id=project_id, dialect='standard')
    if not pull_records_on_focal_repo.empty:
        df = pull_records_on_focal_repo.fillna(0)
        pull_events_actorids=df.astype(int)
        actorids=pull_events_actorids.stack().tolist()
        print("actorsid",actorids)
        print("its length",len(actorids),type(actorids))
        # boolean_series = CC_for_all_repos[CC_for_all_repos.actor_id.isin(actorids)]
        # boolean_series=CC_for_all_repos.loc[CC_for_all_repos['actor_id'].isin(actorids)]
        res = str(actorids)[1:-1]
        print(res)
        CC_result ="select distinct repo_id,actor_id from `github-project-323219.github_analysis_25k.CC_for_all_repos` where actor_id in ("""+res+""" 
            ) and repo_id!="""+str(ids)+""" order by repo_id """
        C_actorids_result = pd.read_gbq(CC_result, project_id=project_id, dialect='standard')
        if not C_actorids_result.empty:
            final_filtered_df=C_actorids_result['repo_id'].unique()
            print("unique",final_filtered_df,type(final_filtered_df))
            ref_repoids=final_filtered_df.tolist()
            print("%%%%%%% list of repoids from a df",ref_repoids)
            PartB_refrepos[ids].append(ref_repoids)
            print("%%%%%% after appending",PartB_refrepos,"length of a list",len(ref_repoids))
            i_j_repos_count=len(ref_repoids)
            PartB_refrepos_count.append([ids,i_j_repos_count])
            print("ref repor count list",PartB_refrepos_count)
        i=i+1


filename_finaldic = "C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\CitingRef_PartB_Outputfiles\\PartB_ref_repos_dict_output.txt"
filename_contricount = "C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\CitingRef_PartB_Outputfiles\\PartB_ref_repo_count.txt"
print("############### type 0f PartB_refrepos ##########",type(PartB_refrepos))
print("##########",PartB_refrepos,"keys",PartB_refrepos.keys(),"values",PartB_refrepos.values())
j=0
with open(filename_finaldic,'w') as output:
    writer = csv.writer(output)
    for k, v in PartB_refrepos.items():
        writer.writerow([k] + v)
        j=j+1
# with open(filename_contricount,'w') as output:
#     writer = csv.writer(output)
#     for i in len(PartB_refrepos_count):
#         writer.writerow(i)

ref_dataframeB = DataFrame (PartB_refrepos_count,columns=['repo_id','count_of_Part_i_j_repos'])
ref_dataframeB.to_gbq('github_25k_repos.Part_i_j_repos_count',
                      project_id,
                      chunksize=None,
                      if_exists='replace'
                      )
print("!!!!!!!!!!created table Part_i_j_repos_count Done!!!!!!!!!")

# partB_refrepos_list_org = []
# print("Started running the new code")
# final_partB_refrepos_list_df_org=pd.DataFrame(PartB_refrepos([ (k,pd.Series(v)) for k,v in PartB_refrepos.items() ])).melt().dropna()
#
# print(final_partB_refrepos_list_df_org.head())
partB_refrepos_list_org=[]
for repoid in PartB_refrepos.keys():
    for value in PartB_refrepos[repoid]:
        print("###value",value,type(value))
        for each_value in value:
            partB_refrepos_list_org.append([repoid,each_value,repoid])
            print(partB_refrepos_list_org)

final_partB_refrepos_list_df_org = pd.DataFrame(partB_refrepos_list_org, columns =['main_repo_id','Part_i_j_refrepos_from','main_repo_id_to'])
print(final_partB_refrepos_list_df_org.head())

final_partB_refrepos_list_df_org.to_gbq('github_25k_repos.Part_i_j_repos_in_detail',
                                        project_id,
                                        chunksize=None,
                                        if_exists='replace'
                                        )
print("!!!!!!!! created table for Part_i_j_repos_in_detail!!!!!!!!")

# # The below code generates the dataframe with main repoid and ref repos as two diff columns note:should convert unique repo variable to str
partA_refrepos_list_temp = []
for repoid in PartB_refrepos.keys():
    for value in PartB_refrepos[repoid]:
        print("###value",value,type(value))
        res=str(value)[1:-1]
        partA_refrepos_list_temp.append([repoid,res])
        print(partA_refrepos_list_temp)

final_partB_refrepos_list_df_temp = pd.DataFrame(partA_refrepos_list_temp, columns =['main_repo_id', 'Part_i_j_refrepos'])
print(final_partB_refrepos_list_df_temp.head())

final_partB_refrepos_list_df_temp.to_gbq('github_25k_repos.Part_i_j_repos',
                                         project_id,
                                         chunksize=None,
                                         if_exists='replace'
                                         )
print("!!!!!!!!!! created table Part_i_j_repos_temp  !!!!!!!!!!!!!!!!")