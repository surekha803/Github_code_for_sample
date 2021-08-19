import itertools

import numpy as np
import pandas as pd
import os

from pandas import DataFrame

project_id = 'github-project-310921'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'github-project-310921-189911271191.json'
sql = "select repo_id,actor_id,min_d,max_d from github-project-310921.github_25k_repos.CC_min_max_for_all_repos"
CC_min_max_dates_df = pd.read_gbq(sql, project_id=project_id, dialect='standard')
print(CC_min_max_dates_df.head())


def unique(list1):
    x = np.array(list1)
    print("unique values",np.unique(x))
    y=np.unique(x).tolist()
    return y

from collections import defaultdict
PartA_refrepos = defaultdict(list)
final_ref_list=[]
distinct_repoid = CC_min_max_dates_df['repo_id'].unique()
repoids = distinct_repoid.tolist()
PartA_ref_list=[]
for each_id in repoids:
    filter_rows_onid=CC_min_max_dates_df[CC_min_max_dates_df['repo_id'] == each_id]
    for index, row in filter_rows_onid.iterrows():
        repo_id=row['repo_id']
        actor_id=row['actor_id']
        min_d=row['min_d']
        max_d=row['max_d']
        sql1="""select  distinct repo_id as ref_repo_of_Focal from github-project-310921.githubproject_rawdataset.githubproject_rawdataset_with_part where created_at >= '""" +str(min_d)+"' and created_at <= '""" + str(max_d)+"' and repo_id !="""+str(repo_id) +" and actor_id="+ str(actor_id)+" and Type='PullRequestEvent'"""
        print("query",sql1)
        PartA_repos= pd.read_gbq(sql1,project_id=project_id, dialect='standard')
        edge_list=PartA_repos.stack().tolist()
        PartA_ref_list.append(edge_list)
    print(PartA_ref_list)
    List_flat = list(itertools.chain(*PartA_ref_list))
    print("list flat",List_flat)
    unique_list_flat=unique(List_flat)
    print("repo list",unique_list_flat)
    if(len(unique_list_flat)!=0):
        uni_repos=unique_list_flat
        print("@@@@@@@@@@ after removing the extra braces@@@@@@@@@@@@",uni_repos)
        PartA_refrepos[each_id].append(uni_repos)
    final_ref_list.append([repo_id,len(uni_repos)])
    print("#############final ref list ",final_ref_list)
    print("########PartA_refrepos######",PartA_refrepos)
    PartA_ref_list.clear()
    list_count=0
print("PartA_refrepos in PartA file",PartA_refrepos)
ref_dataframe = DataFrame (final_ref_list,columns=['repo_id','count_of_repo_references'])

print(ref_dataframe.head())
# ref_dataframe.to_csv(r'C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\CitingRefRepos_PartA_Outputfiles\\repo_ref_df.csv',index=False)
# filename_PartA_refrepos = "C:\\Users\\iialab\\OneDrive - UNT System\\Documents\\Git_analysis\\CitingRefRepos_PartA_Outputfiles\\PartA_refrepos.txt"


ref_dataframe.to_gbq('github_25k_repos.PartA_refrepos_count',
                     project_id,
                     chunksize=None,
                     if_exists='replace'
                     )
print("!!!!!!!!!!! created table PartA_refrepos_count done !!!!!")

partA_refrepos_list_org = []
for repoid in PartA_refrepos.keys():
    for value in PartA_refrepos[repoid]:
        print("###value",value,type(value))
        for each_value in value:
            partA_refrepos_list_org.append([repoid,repoid,each_value])
            print(partA_refrepos_list_org)

final_partA_refrepos_list_df_org = pd.DataFrame(partA_refrepos_list_org, columns =['main_repo','focal_repo_from','PartA_refrepos'])
print(final_partA_refrepos_list_df_org.head())


final_partA_refrepos_list_df_org.to_gbq('github_25k_repos.PartA_ref_repos_in_detail',
                                        project_id,
                                        chunksize=None,
                                        if_exists='replace'
                                        )
print("!!!!!!!!!!! created table PartA_ref_repos done !!!!!")

# The below code generates the dataframe with main repoid and ref repos as two diff columns note:should convert unique repo variable to str
partA_refrepos_list_temp = []
for repoid in PartA_refrepos.keys():
    for value in PartA_refrepos[repoid]:
        print("###value",value,type(value))
        # res=str(value)[1:-1]
        partA_refrepos_list_temp.append([repoid,value])
        print(partA_refrepos_list_temp)

final_partA_refrepos_list_df = pd.DataFrame(partA_refrepos_list_temp, columns =['main_repo_id', 'PartA_refrepos'])
print(final_partA_refrepos_list_df.head())

final_partA_refrepos_list_df.to_gbq('github_25k_repos.PartA_ref_repos',
                                    project_id,
                                    chunksize=None,
                                    if_exists='replace'
                                    )

print("!!!!!!!!!!! created table PartA_ref_repos  done !!!!!")
# The below code is tocreate table with PartA_refrepos data
# print("PartA_refrepos.items() are",PartA_refrepos.items())
# # print each data item.
# PartA_refrepos_df = pd.DataFrame()
# PartA_refrepos_df['focal_repo']=PartA_refrepos.keys()
# PartA_refrepos_df['reference_repos']=str(PartA_refrepos.values())
#
# PartA_refrepos_df.to_gbq('github_25k_repos.PartA_refrepos',
#                          project_id,
#                          chunksize=None,
#                          if_exists='replace'
#                          )
