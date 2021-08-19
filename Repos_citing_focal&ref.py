import csv
import csvwriter as csvwriter
from  google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
from google.cloud import bigquery_storage
import json
import pandas as pd
import os

from pandas import DataFrame

project_id = 'github-project-310921'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'github-project-310921-189911271191.json'
Part_i_j= "select main_repo_id,Part_i_j_refrepos from gigithub-project-323219.github_analysis_25k.Part_i_j_repos where repo_id=27 "
Part_i_j_repos = pd.read_gbq(Part_i_j, project_id=project_id, dialect='standard')
print("df1",Part_i_j_repos.head())

Part_A= "select Focal_repo,ref_repo,actor_id from github-project-323219.github_analysis_25k.focal_repo_refrepos where focal_repo=27 "
Part_A_repos = pd.read_gbq(Part_A, project_id=project_id, dialect='standard')
print("df2",Part_A_repos.head())

CC = "select repo_id,actor_id,min_d,max_d from ggithub-project-323219.github_analysis_25k.CC_for_all_repos"
CC_for_all_repos = pd.read_gbq(CC, project_id=project_id, dialect='standard')
print("df3",CC_for_all_repos.head())

PartC_output_df = []
PartC_output_in_detail = []
i_j_repos_count=[]
for index, row in Part_i_j_repos.iterrows():
    main_repo_id=row['main_repo_id']
    print("main_repo_id is",main_repo_id)
    PartB_refrepos = row['Part_i_j_refrepos']
    print("PartB_refrepos is",PartB_refrepos)
    print(type(main_repo_id),type(PartB_refrepos))
    a_list = PartB_refrepos.split(",")
    map_object = map(int, a_list)
    list_of_integers = list(map_object)
    print("list_of_integers",list_of_integers,type(list_of_integers))
    # actorids_partB_refrepos_df1=CC_for_all_repos[CC_for_all_repos['repo_id'].isin(list_of_integers)]
    # print("@@@@@@@@@@@actorids_partB_refrepos_df1 output",actorids_partB_refrepos_df1.head())
    res = str(list_of_integers)[1:-1]
    print("actor ids who have done pull events on fpcal references",res)
    CC_result ="select distinct repo_id as repo_id_B,actor_id actor_id from `github-project-323219.github_analysis_25k.CC_for_all_repos` where actor_id in ("""+res+""" 
            ) and repo_id!="""+str(main_repo_id)+""" order by repo_id """
    CC_actorids_result = pd.read_gbq(CC_result, project_id=project_id, dialect='standard')
    print("Actorids of Part i_j ",CC_actorids_result.head())
    print("partA_repos_forpartCinput",Part_A_repos.head())
    partA_refrepos=Part_A_repos.loc[Part_A_repos['main_repo_id'] == main_repo_id]
    # b_list=str(partA_refrepos).split(",")
    # map_object_b = map(int, b_list)
    # list_of_integers_b = list(map_object)
    # print("PartA repo output",list_of_integers_b,type(list_of_integers_b))
    # print("type pf partA_refrepos is",partA_refrepos,partA_refrepos.astype(str).values.flatten().tolist())
    print("type of partA_refrepos is",partA_refrepos,type(partA_refrepos))
    if partA_refrepos.empty == False and CC_actorids_result.empty == False:
        ref_values=partA_refrepos.PartA_refrepos.str.split(',')
        print("###############",ref_values,type(ref_values))
        a=ref_values.apply(pd.Series).stack().reset_index(drop = True)
        print("!!!!!!!!!!!!!!",a,type(a))
        b=list(a)
        print("type of b",type(b))
        str1 = list(map(int, b))
        print("^^^^^",type(str1),str1)
        str2=str(str1)[1:-1]
        print("(((((((",str2)
        # if partA_refrepos.empty == False and actorids_partB_refrepos_df1.empty == False:
        #     print("entered in to if loop")
        #     print("partA_refrepos is",partA_refrepos,"actorids_partB_refrepos_df1 is ",actorids_partB_refrepos_df1)
        print("""select  distinct repo.id as repo_id_A,actor.id as actor_id from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+"""
                where  type='PullRequestEvent' and repo_id in ("""+str2+""")""")
        PartA_actor_ids = """select  distinct repo.id as repo_id_A,actor.id as actor_id from """+"`"+"github-project-310921.githubproject_rawdataset.20*"""+"`"+"""
                  where  type='PullRequestEvent' and repo_id in ("""+str2+""")"""
        PartA_pull_actorids = pd.read_gbq(PartA_actor_ids, project_id=project_id, dialect='standard')
        print("actorids_partA_refrepos_df2",Part_A_repos)
        df2_res=PartA_pull_actorids.fillna(0)
        df_a=df2_res.astype(int)
        print("df2_final",df_a)
        result = CC_actorids_result.merge(df2_res, how='inner', on=['actor_id'])
        print("df1",CC_actorids_result.head())
        print("result",result)
        partC_repoids_output=result[["repo_ids_B","repo_ids_A"]]
        print("&&&&&&&&&&&&&&&&final part C output&&&&&&&&&&&&&&",partC_repoids_output,type(partC_repoids_output))
        partc_repoids_in_Detail=result[[main_repo_id,"repo_ids_B","repo_ids_A"]]
        print("&&&&&&&&&&&&&&&&final part C output&&&&&&&&&&&&&&",partc_repoids_in_Detail,type(partc_repoids_in_Detail))
        PartC_output_df.append(partC_repoids_output)
        PartC_output_in_detail.append(partc_repoids_in_Detail)
        PartC_repos_count=result["repo_ids_B"].count()
        j_repos=result["repo_ids_B"].values.tolist()
        i_j_repos=PartB_refrepos.values.tolist()
        i_repos=list(set(i_j_repos)^set(j_repos))
        PartB_repos_count=len(i_repos)
        i_j_repos_count.append([main_repo_id,PartB_repos_count,PartC_repos_count])
        print("i_j_repo_count",i_j_repos_count)
PartC_output_df=pd.concat(PartC_output_df)
print("appended df",PartC_output_df.head())
PartC_output_df.to_csv(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\Subsequentwork_PartC_Outputfiles\\PartC_output.csv',index=False)
PartC_output_in_detail.to_csv(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\Subsequentwork_PartC_Outputfiles\\PartC_output_in_detail.csv',index=False)


PartC_output_df.to_gbq('githubproject_rawdataset.PartC_ref_repos',
                       project_id,
                       chunksize=None,
                       if_exists='replace'
                       )
print("!!!!!!!!!!! created table PartC_output_df done !!!!!")


PartC_output_in_detail.to_gbq('githubproject_rawdataset.PartC_output_in_detail',
                       project_id,
                       chunksize=None,
                       if_exists='replace'
                       )
print("!!!!!!!!!!! created table PartC_output_in_detail done !!!!!")

ref_dataframeB = DataFrame (i_j_repos_count,columns=['main_repo_id','i_count','j_count'])
ref_dataframeB.to_gbq('github_25k_repos.i_j_counts',
                      project_id,
                      chunksize=None,
                      if_exists='replace'
                      )





