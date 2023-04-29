from airflow.models import DAG 
from airflow.operators.bash import BashOperator
from datetime import datetime 


#Define the default_args dictionary
default_args = {
    'Owner' : 'lance',
    'start_date' : datetime(2023,4,26),
    'retries' : 2,
    'schedule_interval' : '@weekly'
}

#Instantiate DAGS
mma_dag = DAG('MMA_DAG', default_args=default_args)

#Define the tasks
t1 = BashOperator(
    task_id = 'UFC_Cards',
    #Define the bash command
    bash_command= 'bash ~/data_science/mma/airflow/ufc_card_scrapy.sh',
    #Add the task to the dag
    dag=mma_dag
)

t2 = BashOperator(
    task_id = 'UFC_Fight_Scorecards',
    #Define the bash command
    bash_command= 'python ~/data_science/mma/ufc_fight_scorecards/ufc_scorecards.py',
    #Add the task to the dag
    dag=mma_dag
)

t3 = BashOperator(
    task_id = 'UFC_Fights',
    #Define the bash command
    bash_command= 'python ~/data_science/mma/ufc_fights/ufc_fights.py',
    #Add the task to the dag
    dag=mma_dag
)

t4 = BashOperator(
    task_id = 'UFC_Fight_Stats',
    #Define the bash command
    bash_command= 'python ~/data_science/mma/ufc_fight_stats/fight_stats.py',
    #Add the task to the dag
    dag=mma_dag
)

t1 >> t3 >> t4