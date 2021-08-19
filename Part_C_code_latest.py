from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
from google.cloud import bigquery_storage
import json
import pandas as pd
import os

from pandas import DataFrame

project_id = 'github-project-323219'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'github-project-323219-d094772f0be4.json'
Part_i_j= "select main_repo_id,Part_i_j_refrepos from github-project-323219.github_25k_repos.Part_i_j_repos where main_repo_id=27 "
Part_i_j_repos = pd.read_gbq(Part_i_j, project_id=project_id, dialect='standard')
print("df1",Part_i_j_repos.head())

Part_A= "select main_repo_id,PartA_refrepos from github-project-323219.github_25k_repos.PartA_ref_repos where main_repo_id=27 "
Part_A_repos = pd.read_gbq(Part_A, project_id=project_id, dialect='standard')
print("df2",Part_A_repos.head())


CC = "select repo_id,actor_id from github-project-323219.github_analysis_25k.CC_for_all_repos"
CC_for_all_repos = pd.read_gbq(CC, project_id=project_id, dialect='standard')
print("df3",CC_for_all_repos.head())

PartC_output_df = []
PartC_output_in_detail = []
i_j_repos_count=[]
separator = ", "
from collections import defaultdict
PartC_refrepos = defaultdict(list)
for index, row in Part_i_j_repos.iterrows():
    main_repo_id=row['main_repo_id']
    print("main_repo_id is",main_repo_id)
    PartB_refrepos = row['Part_i_j_refrepos']
    print("PartB_refrepos is",PartB_refrepos)
    a_list = PartB_refrepos.split(",")
    map_object = map(int, a_list)
    list_of_integers = list(map_object)
    print("list_of_int of part_i_j",list_of_integers,type(list_of_integers))
    str_part_i_j_values = str(list_of_integers)[1:-1]
    CC_repo_id_result ="select distinct repo_id as repo_id_B,actor_id actor_id from `github-project-323219.github_analysis_25k.CC_for_all_repos` where repo_id in ("""+str_part_i_j_values+""" 
            ) order by repo_id """
    print(CC_repo_id_result)
    CC_actorids_result = pd.read_gbq(CC_repo_id_result, project_id=project_id, dialect='standard')
    print("CC details for Part i_j repos ",CC_actorids_result.head())
    print("partA_repos_forpartCinput",Part_A_repos.head())
    partA_refrepos=Part_A_repos.loc[Part_A_repos['main_repo_id'] == main_repo_id]
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
        print("str of  partA reference repos",str2)
        print("""select  distinct repo_id as repo_id_A,actor_id as actor_id from github-project-323219.github_analysis_25k.github_dataset_with_part
                where  type='PullRequestEvent' and repo_id in ("""+str2+""")""")
        PartA_actor_ids = """select  distinct repo_id as repo_id_A,actor_id as actor_id from github-project-323219.github_analysis_25k.github_dataset_with_part
                  where  type='PullRequestEvent' and repo_id in ("""+str2+""")"""
        PartA_pull_actorids = pd.read_gbq(PartA_actor_ids, project_id=project_id, dialect='standard')
        print("actorids_partA_refrepos_df2",Part_A_repos)
        df2_res=PartA_pull_actorids.fillna(0)
        df_a=df2_res.astype(int)
        print("actors who have done pull on ref repos",df_a)
        result = CC_actorids_result.merge(df2_res, how='inner', on=['actor_id'])
        print("df1",CC_actorids_result.head())
        print("result",result)
        if not result.empty:
            partC_repoids_output=result[["repo_id_B","repo_id_A"]]
            print("&&&&&&&&&&&&&&&&final part C output&&&&&&&&&&&&&&",partC_repoids_output,type(partC_repoids_output))
            partc_repoids_in_Detail=result[["repo_id_B","repo_id_A"]]
            # partc_repoids_in_Detail=partc_repoids_in_Detail["main_repo_id"]=main_repo_id
            partc_repoids_in_Detail.loc[:,'main_repo_id'] = main_repo_id
            print("&&&&&&&&&&&&&&&&final part C output&&&&&&&&&&&&&&",partc_repoids_in_Detail.head(),type(partc_repoids_in_Detail))
            print("partC_repoids_output",partC_repoids_output.head())
            PartC_output_df.append(partC_repoids_output)
            PartC_output_in_detail.append(partc_repoids_in_Detail)
            print("partc_repoids_in_Detail",partc_repoids_in_Detail.head())
            PartC_repos_count=result["repo_id_B"].count()
            j_repos=result["repo_id_B"].unique().tolist()
            result.to_csv(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\Subsequentwork_PartC_Outputfiles\\PartC_1.csv',index=False)
            PartC_refrepos[main_repo_id].append(j_repos)
            print("j_repos",j_repos)
            i_j_repos=list_of_integers
            print("i_j_repos",len(list_of_integers),type(list_of_integers),i_j_repos)
            # i_repos=list(set(i_j_repos)^set(j_repos))
            i_j_list_as_set=set(i_j_repos)
            intersection_output=i_j_list_as_set.intersection(j_repos)
            print("intersection_output",intersection_output)
            common_repos_count=len(intersection_output)
            print("common_repos_count",common_repos_count)
            PartB_repos_count=len(i_j_repos) - len(intersection_output)
            print("PartB_repos_count",PartB_repos_count)
            i_j_repos_count.append([main_repo_id,PartB_repos_count,common_repos_count])
            print("i_j_repo_count",i_j_repos_count)
PartC_output_df=pd.concat(PartC_output_df)
print("appended df",PartC_output_df.head())
PartC_output_df.to_csv(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\Subsequentwork_PartC_Outputfiles\\PartC_output.csv',index=False)
# final_partC_refrepos_list_df_org = pd.DataFrame(PartC_output_in_detail)
# print(final_partC_refrepos_list_df_org.head())


partc_repoids_in_Detail.to_csv(r'C:\Users\iialab\OneDrive - UNT System\Documents\Git_analysis\Subsequentwork_PartC_Outputfiles\\PartC_output_in_detail.csv',index=False)


PartC_output_df.to_gbq('githubproject_rawdataset.PartC_ref_repos',
                       project_id,
                       chunksize=None,
                       if_exists='replace'
                       )
print("!!!!!!!!!!! created table PartC_output_df done !!!!!")


partc_repoids_in_Detail.to_gbq('githubproject_rawdataset.PartC_output_in_detail',
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





