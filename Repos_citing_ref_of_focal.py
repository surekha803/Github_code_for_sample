import numpy as np
import pandas as pd
from  google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
from google.cloud import bigquery_storage
import os

from pandas import DataFrame

project_id = 'github-project-310921'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'github-project-310921-189911271191.json'
Part_A= "select main_repo_id,PartA_refrepos from github-project-310921.github_25k_repos.PartA_ref_repos where repo_id=27 "
Part_A_repos = pd.read_gbq(Part_A, project_id=project_id, dialect='standard')
print("df2",Part_A_repos.head())

CC = "select repo_id,actor_id,min_d,max_d from github-project-310921.github_25k_repos.CC_for_all_repos"
CC_for_all_repos = pd.read_gbq(CC, project_id=project_id, dialect='standard')
print("df3",CC_for_all_repos.head())

Part_B= "select main_repo_id,PartB_refrepos from github-project-310921.github_25k_repos.PartB_ref_repos where repo_id=27 "
Part_B_repos = pd.read_gbq(Part_B, project_id=project_id, dialect='standard')
print("df2",Part_B_repos.head())
partB_dic=Part_B_repos.to_dict()

PartD_output_df=[]
PartD_repo_counts=[]

for index, row in Part_A_repos.iterrows():
    main_repo_id=row['main_repo_id']
    PartA_refrepos=row['PartA_refrepos']
    print(PartA_refrepos)
    print("type of b",type(PartA_refrepos))
    PartA_refrepos_list=PartA_refrepos.split(",")
    print("type of b_list",type(PartA_refrepos_list))
    print("""select  distinct repo.id as repo_id_A,actor.id as actor_id from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+"""
                where  type='PullRequestEvent' and repo.id in ("""+PartA_refrepos_list+""")""")
    partA_refrepos_actorids_df2 = """select  distinct repo.id as repo_id_A,actor.id as actor_id from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+"""
                  where  type='PullRequestEvent' and repo.id in ("""+PartA_refrepos_list+""")"""
    df2=partA_refrepos_actorids_df2.fillna(0)
    partA_refrepos_actorids_df2_final=df2.astype(int)
    print("df2_final",partA_refrepos_actorids_df2_final)
    PartA_actors_pullevents=partA_refrepos_actorids_df2_final['actor_id'].tolist()
    print("!!!!!!!!!!!",PartA_actors_pullevents)
    CC_output=CC_for_all_repos[CC_for_all_repos['actor_id'].isin(PartA_actors_pullevents)]
    print("@@@@@@@@",CC_output)
    CC_output_final=CC_output[~CC_output['repo_id'].isin(PartA_refrepos_list)]
    print("&&&&&&&",CC_output_final)
    result = partA_refrepos_actorids_df2_final.merge(CC_output_final, how='inner', on=['actor_id'])
    print("df2",partA_refrepos_actorids_df2_final.head())
    print("CC_output_final",CC_output_final.head())
    print("result",result)
    partD_repoids_output=result[["repo_id","repo_id_A"]]
    PartD_output_df.append(partD_repoids_output)
    Part_C_repos=partB_dic[main_repo_id].tolist()
    Part_D_repos=partD_repoids_output["repo_id"].values().tolist()
    k_repos=list(set(Part_C_repos)^set(Part_D_repos))
    k_count=len(k_repos)
    PartD_repo_counts.append(main_repo_id,k_count)

PartD_output_df=pd.concat(PartD_output_df)
print("appended df",PartD_output_df.head())
PartD_output_df.to_gbq('githubproject_rawdataset.PartD_ref_repos_final_test',
                       project_id,
                       chunksize=None,
                       if_exists='replace'
                       )
print("!!!!!!!!!!! created table partD_repoids_output done !!!!!")

repo_citing_to_ref= DataFrame (PartD_repo_counts,columns=['main_repo_id','k_count'])
repo_citing_to_ref.to_gbq('github_25k_repos.k_repo_count',
                      project_id,
                      chunksize=None,
                      if_exists='replace'
                      )