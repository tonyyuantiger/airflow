from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor

with DAG(
    dag_id="SS_billing_pipeline",
    schedule='45 16 * * 1-5',
    start_date=pendulum.datetime(2023, 12, 6, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["Short_Sell"]
    # default_args={"retries": 5}
):
    Preferred_margin = BashOperator(
        task_id="Preferred_margin",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Short_Sell/preferred_margin_Ver2.py >> /home/script.master/Documents/Log_files/preferred_margin.txt 2>&1"
    )

    Reference_rate_generation = BashOperator(
        task_id="Reference_rate_generation",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Short_Sell/Reference_rate_generation_Ver2.py >> /home/script.master/Documents/Log_files/Reference_rate_generation_Ver2.txt 2>&1"
    )

    Reference_rate_upload = BashOperator(
        task_id="Reference_rate_upload",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Short_Sell/SS_Reference_Rate_Upload_Ver3.py >> /home/script.master/Documents/Log_files/SS_Reference_Rate_Upload_Ver3.txt 2>&1"
    )

    Per_symbol_income = BashOperator(
        task_id="Per_symbol_income",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Short_Sell/per_symbol_income.py >> /home/script.master/Documents/Log_files/per_symbol_income.txt 2>&1"
    )

    Shorts_with_rates = BashOperator(
        task_id="Shorts_with_rates_markup",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Short_Sell/Shorts_with_rates_markup_Ver3.py >> /home/script.master/Documents/Log_files/Shorts_with_rates_markup.txt 2>&1"
    )

    SS_billing = BashOperator(
        task_id="SS_billing_generation",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Short_Sell/Marsco_SS_interest_SL_Ver5.py >> /home/script.master/Documents/Log_files/Marsco_SS_interest_SL.txt 2>&1"
    )

    NZSG_billing_1 = BashOperator(
        task_id="NZSG_billing_generation",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Short_Sell/NZSG_billing_generation_Ver1.py >> /home/script.master/Documents/Log_files/NZSG_billing_generation_Ver1.txt 2>&1"
    )

    NZSG_billing_2 = BashOperator(
        task_id="NZSG_billing_upload",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Short_Sell/NZSG_billing_upload_Ver1.py >> /home/script.master/Documents/Log_files/NZSG_billing_upload_Ver1.txt 2>&1"
    )

    Per_symbol_income.set_downstream(Preferred_margin)
    Shorts_with_rates.set_downstream([SS_billing, Reference_rate_generation])
    Reference_rate_generation.set_downstream(Reference_rate_upload)
    SS_billing.set_downstream(NZSG_billing_1)
    NZSG_billing_1.set_downstream(NZSG_billing_2)

with DAG(
    dag_id="FPL_pipeline",
    schedule='0 8 * * 2-6',
    start_date=pendulum.datetime(2023, 12, 5, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["FPL"]
    # default_args={"retries": 5}
):
    FPL_EOD = BashOperator(
        task_id="FPL_EOD",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPL_EOD_Ver2.py >> /home/script.master/Documents/Log_files/FPL_EOD.txt 2>&1"
    )

    FPL_Reconcile = BashOperator(
        task_id="FPL_Reconcile",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPL_Reconcile.py >> /home/script.master/Documents/Log_files/FPL_Reconcile.txt 2>&1"
    )

    FPL_EOD.set_downstream(FPL_Reconcile)


with DAG(
    dag_id="locate_statement_pipeline",
    schedule='10 2 * * 2-6',
    start_date=pendulum.datetime(2023, 8, 7, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=2,
    tags=["locate_statement"]
    #default_args={"retries": 5}
):
    smartloan_eod_download = BashOperator(
        task_id="smartloan_eod_download",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/SL_related/Smartloan_EOD_download.py >> /home/script.master/Documents/Log_files/Smartloan_EOD_download.txt 2>&1"
    )

    smartloan_eod_to_s3 = BashOperator(
        task_id="smartloan_eod_to_s3",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/SL_related/Smartloan_S3.py >> /home/script.master/Documents/Log_files/Smartloan_S3.txt 2>&1"
    )

    short_authorization_ingestion = BashOperator(
        task_id="short_authorization_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/ShortAuthorizations_insert.py >> /home/script.master/Documents/Log_files/ShortAuthorizations_insert.txt 2>&1"
    )

    locate_statement = BashOperator(
        task_id="locate_statement",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Short_Sell/locate_statement.py >> /home/script.master/Documents/Log_files/locate_statement.txt 2>&1"
    )
    smartloan_eod_download.set_downstream([short_authorization_ingestion])
    short_authorization_ingestion.set_downstream([locate_statement])

with DAG(
    dag_id="Broadridge_pipeline",
    schedule='30 1 * * 2-6',
    start_date=pendulum.datetime(2023, 8, 7, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["Broadridge"]
    #default_args={"retries": 5}
):
    BR_eod_download = BashOperator(
        task_id="BR_eod_download",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/BR_EOD_download.py >> /home/script.master/Documents/Log_files/BR_EOD_download.txt 2>&1",
    )

    # FDID_upload = BashOperator(
    #     task_id="FDID_upload",
    #     bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Name_and_Address/FDID_upload.py >> /home/script.master/Documents/Log_files/FDID_upload.txt 2>&1",
    # )

    # br_eod_upload = BashOperator(
    #     task_id="br_eod_upload",
    #     bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/br_eod_upload.py >> /home/script.master/Documents/Log_files/br_eod_upload.txt 2>&1",
    # )

    softek_upload = BashOperator(
        task_id="softek_upload",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python3 /home/script.master/Documents/Python_scripts/CP_related/softek_transmission_Ver2.py >> /home/script.master/Documents/Log_files/softek_transmission.txt 2>&1",
    )

    CUSBAL_Parser = BashOperator(
       task_id="CUSBAL_Parser",
       bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/CUSBAL_Parser.py >> /home/script.master/Documents/Log_files/CUSBAL_Parser.txt 2>&1"
    )

    CUSHOL_loader = BashOperator(
       task_id="CUSHOL_loader",
       bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/CUSHOL_loader.py >> /home/script.master/Documents/Log_files/CUSHOL_loader.txt 2>&1",
    )

    TACT_loader = BashOperator(
        task_id="TACT_loader",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/TACT_loader.py >> /home/script.master/Documents/Log_files/TACT_loader.txt 2>&1",
    )

    SRCNSFA_loader = BashOperator(
        task_id="SRCNSFA_loader",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/SRCNSFA_parsing.py >> /home/script.master/Documents/Log_files/SRCNSFA_parsing.txt 2>&1",
    )


    CRREQ_parsing = BashOperator(
        task_id="CRREQ_parsing",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/CRREQ_parsing.py >> /home/script.master/Documents/Log_files/CRREQ_parsing.txt 2>&1"
    )

    DBREQ_parsing = BashOperator(
        task_id="DBREQ_parsing",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/DBREQ_parsing.py >> /home/script.master/Documents/Log_files/DBREQ_parsing.txt 2>&1"
    )

    MRGSEC_parsing = BashOperator(
        task_id="MRGSEC_parsing",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/MRGSEC_parser_Ver2.py >> /home/script.master/Documents/Log_files/MRGSEC_parser.txt 2>&1"
    )

    TACT_ingestion = BashOperator(

        task_id="TACT_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/TACT_ingestion.py >> /home/script.master/Documents/Log_files/TACT_ingestion.txt 2>&1",
    )

    CUSBAL_ingestion = BashOperator(
        task_id="CUSBAL_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/CUSBAL_ingestion.py >> /home/script.master/Documents/Log_files/CUSBAL_ingestion.txt 2>&1",
    )

    CUSHOL_ingestion = BashOperator(
        task_id="CUSHOL_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/CUSHOL_ingestion.py >> /home/script.master/Documents/Log_files/CUSHOL_ingestion.txt 2>&1",
    )

    CRREQ_ingestion = BashOperator(
        task_id="CRREQ_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/CRREQ_ingestion_Ver2.py >> /home/script.master/Documents/Log_files/CRREQ_ingestion.txt 2>&1",
    )

    split_EOD_ingestion = BashOperator(
        task_id="split_EOD_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/split_EOD_ingestion.py >> /home/script.master/Documents/Log_files/split_EOD_ingestion.txt 2>&1",
    )

    journal_movement_ingestion = BashOperator(
        task_id="journal_movement_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/journal_movement_ingestion.py >> /home/script.master/Documents/Log_files/journal_movement_ingestion.txt 2>&1",
    )

    asset_transfer_3i = BashOperator(
        task_id="asset_transfer",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/3i/3i_asset_transfer.py >> /home/script.master/Documents/Log_files/3i_asset_transfer.txt 2>&1",
    )

    balance_3i = BashOperator(
        task_id="balance",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/3i/3i_balance.py >> /home/script.master/Documents/Log_files/3i_balance.txt 2>&1",
    )

    cash_activity_3i = BashOperator(
        task_id="cash_activity",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/3i/3i_cash_activity.py >> /home/script.master/Documents/Log_files/3i_cash_activity.txt 2>&1",
    )

    corporate_action_3i = BashOperator(
        task_id="corporate_action",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/3i/3i_corporate_action.py >> /home/script.master/Documents/Log_files/3i_corporate_action.txt 2>&1",
    )

    open_position_3i = BashOperator(
        task_id="open_position",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/3i/3i_open_position.py >> /home/script.master/Documents/Log_files/3i_open_position.txt 2>&1",
    )

    trade_activity_3i = BashOperator(
        task_id="trade_activity",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/3i/3i_trade_activity.py >> /home/script.master/Documents/Log_files/3i_trade_activity.txt 2>&1",
    )

    EOD_auto_email_3i = BashOperator(
        task_id="EOD_auto_email",
        bash_command="date",
    )

    knowtice = BashOperator(
        task_id="knowtice",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/CP_related/knowtice.py >> /home/script.master/Documents/Log_files/knowtice.txt 2>&1",
    )

    idle_cash_income_insertion = BashOperator(
        task_id="idle_cash_income_insertion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/BP_related/board_report/idle_cash_income_insertion.py >> /home/script.master/Documents/Log_files/idle_cash_income_insertion.txt 2>&1",
    )

    FPL_801_journal_movement = BashOperator(
        task_id="FPL_801_journal_movement",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPL_801_Journal_movement.py >> /home/script.master/Documents/Log_files/FPL_801_Journal_movement.txt 2>&1",
    )
    
    COMBOPT_parsing = BashOperator(
        task_id="COMBOPT_parsing",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/COMBOPT_parsing.py >> /home/script.master/Documents/Log_files/COMBOPT_parsing.txt 2>&1"
    )
    
    Negative_balance_alert = BashOperator(
        task_id="Negative_balance_alert",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/Negative_balance_alert.py >> /home/script.master/Documents/Log_files/Negative_balance_alert.txt 2>&1"
    )
    
    # SL_recall = BashOperator(
    #     task_id="SL_recall",
    #     bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/SL_related/recall.py >> /home/script.master/Documents/Log_files/recall.txt 2>&1"
    # )


    BR_eod_download.set_downstream([
         TACT_loader,CUSHOL_loader,CUSBAL_Parser,CRREQ_parsing,DBREQ_parsing,SRCNSFA_loader,MRGSEC_parsing, COMBOPT_parsing])
    CUSHOL_loader.set_downstream(CUSHOL_ingestion)
    CUSHOL_ingestion.set_downstream([open_position_3i, Negative_balance_alert])
    CUSBAL_Parser.set_downstream(CUSBAL_ingestion)
    CUSBAL_ingestion.set_downstream([balance_3i, idle_cash_income_insertion])
    TACT_loader.set_downstream([TACT_ingestion])
    TACT_ingestion.set_downstream([split_EOD_ingestion, journal_movement_ingestion, cash_activity_3i, corporate_action_3i])
    split_EOD_ingestion.set_downstream([trade_activity_3i, asset_transfer_3i])
    asset_transfer_3i.set_upstream([split_EOD_ingestion, CUSHOL_ingestion])
    EOD_auto_email_3i.set_upstream([asset_transfer_3i, trade_activity_3i, open_position_3i, balance_3i, cash_activity_3i, corporate_action_3i])
    CRREQ_parsing.set_downstream(CRREQ_ingestion)
    journal_movement_ingestion.set_downstream([knowtice, FPL_801_journal_movement])
    softek_upload.set_upstream([CUSHOL_ingestion, CUSBAL_ingestion])
    # FDID_upload.set_upstream(NA_ingestion)







with DAG(
    dag_id="Flextrade_pipeline",
    schedule='10 21 * * 1-5',
    start_date=pendulum.datetime(2023, 8, 7, tz='America/Toronto'),
    catchup=False,
    tags=["Flextrade"],
    max_active_tasks=1,
    default_args={"retries": 1}
):
    Flextrade_eod_download = BashOperator(
        task_id="Flextrade_eod_download",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/Flextrade_eod_download_T+0.py >> /home/script.master/Documents/Log_files/Flextrade_eod_download_T+0.txt 2>&1"
    )

    Flextrade_TAT_insert = BashOperator(
        task_id="Flextrade_TAT_insert",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/Flextrade_TradeAuditTrail_insert.py >> /home/script.master/Documents/Log_files/Flextrade_TradeAuditTrail_insert.txt 2>&1"
    )

    Flextrade_ActivityReport_ingestion = BashOperator(
        task_id="Flextrade_ActivityReport_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/Flextrade_ActivityReport_ingestion.py >> /home/script.master/Documents/Log_files/Flextrade_ActivityReport_ingestion.txt 2>&1"
    )

    Flextrade_ExecutionLedger_ingestion = BashOperator(
        task_id="Flextrade_ExecutionLedger_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/Flextrade_equity_executionLedger.py >> /home/script.master/Documents/Log_files/Flextrade_equity_executionLedger.txt 2>&1"
    )

    Flextrade_Opions_ExecutionLedger_ingestion = BashOperator(
        task_id="Flextrade_Opions_ExecutionLedger_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/Flextrade_option_executionLedger_ingestion.py >> /home/script.master/Documents/Log_files/Flextrade_option_executionLedger_ingestion.txt 2>&1"
    )

    Flextrade_Options_ActivityReport_ingestion = BashOperator(
        task_id="Flextrade_Options_ActivityReport_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/Flextrade_option_activity_ingestion.py >> /home/script.master/Documents/Log_files/Flextrade_option_activity_ingestion.txt 2>&1"
    )

    Flextrade_TAT_FRAC = BashOperator(
        task_id="Flextrade_TAT_FRAC",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/Flextrade_TradeAuditTrail_FRAC.py >> /home/script.master/Documents/Log_files/Flextrade_TradeAuditTrail_FRAC.txt 2>&1"
    )

    MRSC_Net_Cash = BashOperator(
        task_id="MRSC_Net_Cash",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/mrsc_net_cash.py >> /home/script.master/Documents/Log_files/mrsc_net_cash.txt 2>&1"
    )

    Flextrade_EOD_upload = BashOperator(
        task_id="Flextrade_EOD_upload",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python3 /home/script.master/Documents/Python_scripts/Flextrade/Flextrade_EOD_upload.py >> /home/script.master/Documents/Log_files/Flextrade_EOD_upload.txt 2>&1"
    )

    buffer = TimeDeltaSensor(task_id='5mins_buffer', delta=timedelta(minutes=5))

    Flextrade_eod_download.set_downstream([Flextrade_ActivityReport_ingestion, Flextrade_ExecutionLedger_ingestion,
            Flextrade_Opions_ExecutionLedger_ingestion, Flextrade_Options_ActivityReport_ingestion])
    Flextrade_TAT_insert.set_downstream([MRSC_Net_Cash,Flextrade_EOD_upload])
    buffer.set_upstream([Flextrade_ActivityReport_ingestion, Flextrade_ExecutionLedger_ingestion,
                                     Flextrade_Opions_ExecutionLedger_ingestion, Flextrade_Options_ActivityReport_ingestion])
    buffer.set_downstream([Flextrade_TAT_insert, Flextrade_TAT_FRAC])
    # Flextrade_TAT_FRAC.set_upstream([Flextrade_ActivityReport_ingestion, Flextrade_ExecutionLedger_ingestion,
    #                                   Flextrade_Opions_ExecutionLedger_ingestion, Flextrade_Options_ActivityReport_ingestion])

with DAG(
    dag_id="LFA_UAT_pipeline",
    schedule='30 4 * * 2-6',
    start_date=pendulum.datetime(2023, 12, 5, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["LFA_UAT"]
):
    LFA_Reports_download_UAT = BashOperator(
        task_id="LFA_Reports_download_UAT",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/LFA_Reports_download_UAT.py >> /home/script.master/Documents/Log_files/LFA_Reports_download_UAT.txt 2>&1"
    )

    LFA_REV_Parsing_UAT = BashOperator(
        task_id="LFA_REV_Parsing_UAT",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/LFA_REV_Parsing_UAT.py >> /home/script.master/Documents/Log_files/LFA_REV_Parsing_UAT.txt 2>&1"
    )

    LFA_Reports_download_UAT.set_downstream(LFA_REV_Parsing_UAT)

with DAG(
    dag_id="FPL_UAT_pipeline",
    schedule='0 18 * * 1-5',
    start_date=pendulum.datetime(2023, 12, 5, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["FPL_UAT"]
):
    FPL_Collateral_Reports_UAT = BashOperator(
        task_id="FPL_Collateral_Reports_UAT",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPL_Collateral_Reports_UAT.py >> /home/script.master/Documents/Log_files/FPL_Collateral_Reports_UAT.txt 2>&1"
    )

    FPLRPT_Parsing_UAT = BashOperator(
        task_id="FPLRPT_Parsing_UAT",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPLRPT_Parsing_UAT.py >> /home/script.master/Documents/Log_files/FPLRPT_Parsing_UAT.txt 2>&1"
    )

    FPLCOLS_Parsing_UAT = BashOperator(
        task_id="FPLCOLS_Parsing_UAT",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPLCOLS_Parsing_UAT.py >> /home/script.master/Documents/Log_files/FPLCOLS_Parsing_UAT.txt 2>&1"
    )

    FPL_Collateral_Reports_UAT.set_downstream([FPLRPT_Parsing_UAT, FPLCOLS_Parsing_UAT])


with DAG(
    dag_id="SYEP_FD_pipeline",
    schedule='50 15 * * 1-5',
    start_date=pendulum.datetime(2023, 12, 5, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["SYEP"]
    # default_args={"retries": 5}
):
    FPL_Collateral_Reports = BashOperator(
        task_id="FPL_Collateral_Reports",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPL_Collateral_Reports_Ver2.py >> /home/script.master/Documents/Log_files/FPL_Collateral_Reports.txt 2>&1"
    )

    FPLRPT_Parsing_and_Upload = BashOperator(
        task_id="FPLRPT_Parsing_and_Upload",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPLRPT_Parsing_and_Upload_Ver3.py >> /home/script.master/Documents/Log_files/FPLRPT_Parsing_and_Upload.txt 2>&1"
    )

    FPLCOLS_Parsing = BashOperator(
        task_id="FPLCOLS_Parsing",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPLCOLS_Parsing.py >> /home/script.master/Documents/Log_files/FPLCOLS_Parsing.txt 2>&1"
    )

    FPL_Collateral_Reports.set_downstream([FPLRPT_Parsing_and_Upload, FPLCOLS_Parsing])

with DAG(
    dag_id="NA_pipeline",
    schedule='0 20 * * 1-5',
    start_date=pendulum.datetime(2023, 12, 5, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["Name&Address"]
    # default_args={"retries": 5}
):

    BR_SameDay_EOD_download = BashOperator(
        task_id="BR_SameDay_EOD_download",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/BR_SameDay_EOD_download.py >> /home/script.master/Documents/Log_files/BR_SameDay_EOD_download.txt 2>&1"
    )

    NA_ingestion = BashOperator(
        task_id="NA_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Name_and_Address/NA_pipeline_Ver7.py >> /home/script.master/Documents/Log_files/NA_pipeline.txt 2>&1"
    )

    New_NA_ingestion = BashOperator(
        task_id="New_NA_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Name_and_Address/new_nnad_ingestion.py >> /home/script.master/Documents/Log_files/new_nnad_ingestion.txt 2>&1"
    )
    
    na_1100g_ingestion = BashOperator(
        task_id="na_1100g_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Name_and_Address/na_1100g.py >> /home/script.master/Documents/Log_files/na_1100g.txt 2>&1"
    )
    
    na_5100e_ingestion = BashOperator(
        task_id="na_5100e_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Name_and_Address/na_5100e.py >> /home/script.master/Documents/Log_files/na_5100e.txt 2>&1"
    )

    FDID_upload = BashOperator(
        task_id="FDID_upload",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Name_and_Address/FDID_upload.py >> /home/script.master/Documents/Log_files/FDID_upload.txt 2>&1"
    )

    TID_upload = BashOperator(
        task_id="TID_upload",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Broadridge/TID_hash_and_upload_full.py >> /home/script.master/Documents/Log_files/TID_hash_and_upload_full.txt 2>&1"
    )
    
    fpl_opt_in = BashOperator(
        task_id="fpl_opt_in",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/fpl_opt_in.py >> /home/script.master/Documents/Log_files/fpl_opt_in.txt 2>&1"
    )


    BR_SameDay_EOD_download.set_downstream([NA_ingestion, New_NA_ingestion, na_1100g_ingestion, na_5100e_ingestion])
    NA_ingestion.set_downstream(FDID_upload)
    na_1100g_ingestion.set_downstream(TID_upload)
    na_5100e_ingestion.set_downstream(fpl_opt_in)


with DAG(
    dag_id="Reconciliation_Break",
    schedule='50 5 * * 2-6',
    start_date=pendulum.datetime(2023, 12, 6, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["Reconciliation"]
    # default_args={"retries": 5}
):
    Recon_Break = BashOperator(
        task_id="Recon_Break",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python3 /home/script.master/Documents/Python_scripts/Flextrade/Recon_break_Ver2.py >> /home/script.master/Documents/Log_files/Recon_break_Ver2.txt 2>&1"
    )

    Flextrade_Position_Recon = BashOperator(
        task_id="Flextrade_Position_Recon",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/FlexTrade_Position_Recon.py >> /home/script.master/Documents/Log_files/Position_Recon_break.txt 2>&1"
    )

    Flextrade_Trade_Recon = BashOperator(
        task_id="Flextrade_Trade_Recon",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Flextrade/Flextrade_Trade_Recon.py >> /home/script.master/Documents/Log_files/Flex_Trade_Recon_break.txt 2>&1"
    )

    blue_ocean_eod_download  = BashOperator(
        task_id="blue_ocean_eod_download",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/BlueOcean/blue_ocean_eod_download.py >> /home/script.master/Documents/Log_files/blue_ocean_eod_download.txt 2>&1"
    )

    blue_ocean_trade_audit_trail_insert = BashOperator(
        task_id="blue_ocean_trade_audit_trail_insert",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/blue_ocean_trade_audit_trail_insert.py >> /home/script.master/Documents/Log_files/blue_ocean_trade_audit_trail_insert.txt 2>&1"
    )

    blue_ocean_activity_report_insert = BashOperator(
        task_id="blue_ocean_activity_report_insert",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/blue_ocean_activity_report_insert.py >> /home/script.master/Documents/Log_files/blue_ocean_activity_report_insert.txt 2>&1"
    )

    blue_ocean_eod_download.set_downstream([blue_ocean_trade_audit_trail_insert,blue_ocean_activity_report_insert])
    blue_ocean_trade_audit_trail_insert.set_downstream([Recon_Break, Flextrade_Position_Recon, Flextrade_Trade_Recon])


with DAG(
    dag_id="blue_ocean_monday",
    schedule='50 5 * * 1',
    start_date=pendulum.datetime(2023, 12, 6, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["blue_ocean"]
    # default_args={"retries": 5}
):


    blue_ocean_eod_download  = BashOperator(
        task_id="blue_ocean_eod_download",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/BlueOcean/blue_ocean_eod_download.py >> /home/script.master/Documents/Log_files/blue_ocean_eod_download.txt 2>&1"
    )

    blue_ocean_trade_audit_trail_insert = BashOperator(
        task_id="blue_ocean_trade_audit_trail_insert",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/blue_ocean_trade_audit_trail_insert.py >> /home/script.master/Documents/Log_files/blue_ocean_trade_audit_trail_insert.txt 2>&1"
    )

    blue_ocean_activity_report_insert = BashOperator(
        task_id="blue_ocean_activity_report_insert",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/blue_ocean_activity_report_insert.py >> /home/script.master/Documents/Log_files/blue_ocean_activity_report_insert.txt 2>&1"
    )

    blue_ocean_eod_download.set_downstream([blue_ocean_trade_audit_trail_insert,blue_ocean_activity_report_insert])

with DAG(
    dag_id="usts_valdi",
    schedule='30 2 * * 2-6',
    start_date=pendulum.datetime(2023, 12, 6, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["usts_valdi"]
    # default_args={"retries": 5}
):

    valdi_eod_download  = BashOperator(
        task_id="valdi_eod_download",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/USTS/Valdi/Valdi_EOD_download.py >> /home/script.master/Documents/Log_files/Valdi_EOD_download.txt 2>&1"
    )

    TAT_ingestion  = BashOperator(
        task_id="TAT_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/USTS/Valdi/TAT_ingestion.py >> /home/script.master/Documents/Log_files/TAT_ingestion.txt 2>&1"
    )

    Valdi_TTS0165_ingestion = BashOperator(
        task_id="Valdi_TTS0165_ingestiont",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/USTS/Valdi/Valdi_TTS0165_ingestion.py >> /home/script.master/Documents/Log_files/Valdi_TTS0165_ingestion.txt 2>&1"
    )

    trade_audit_trail_ingestion  = BashOperator(
        task_id="trade_audit_trail_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Database_related/usts_valdi_trade_audit_trail_ingestion.py >> /home/script.master/Documents/Log_files/usts_valdi_trade_audit_trail_ingestion.txt 2>&1"
    )



    valdi_eod_download.set_downstream([TAT_ingestion, Valdi_TTS0165_ingestion, trade_audit_trail_ingestion])

with DAG(
    dag_id="rebate_report",
    schedule='00 12 * * 1-5',
    start_date=pendulum.datetime(2023, 12, 6, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=2,
    tags=["rebate_repor"]
    # default_args={"retries": 5}
):

    pfof_rebate  = BashOperator(
        task_id="pfof_rebate",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Rebate/pfof_rebate.py >> /home/script.master/Documents/Log_files/pfof_rebate.txt 2>&1"
    )

    rebate_daily_report  = BashOperator(
        task_id="rebate_daily_report",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Rebate/rebate_daily_report.py >> /home/script.master/Documents/Log_files/rebate_daily_report.txt 2>&1"
    )

with DAG(
    dag_id="FPL_EOD_pipeline",
    schedule='40 16 * * 1-5',
    start_date=pendulum.datetime(2023, 12, 5, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["FPL_EOD"]
    # default_args={"retries": 5}
):
    FPL_EOD = BashOperator(
        task_id="FPL_EOD",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPL_EOD_new.py >> /home/script.master/Documents/Log_files/FPL_EOD.txt 2>&1"
    )

    FPL_EOD_Upload = BashOperator(
        task_id="FPL_EOD_Upload",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/FPL_EOD_upload.py >> /home/script.master/Documents/Log_files/FPL_EOD_upload.txt 2>&1"
    )

    FPL_EOD.set_downstream(FPL_EOD_Upload)

with DAG(
    dag_id="MM_EOD_download",
    schedule='00 23 * * 1-5',
    start_date=pendulum.datetime(2023, 12, 6, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["MM_EOD"]
    # default_args={"retries": 5}
):

    pfof_rebate  = BashOperator(
        task_id="MM_EOD",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/Rebate/MM_EOD_download.py >> /home/script.master/Documents/Log_files/MM_EOD_download.txt 2>&1"
    )

with DAG(
    dag_id="data_quality",
    schedule='30 06 * * 2-6',
    start_date=pendulum.datetime(2023, 12, 6, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["data_quality"],
    default_args={"retries": 5}
):

    data_quality_daily  = BashOperator(
        task_id="data_quality_daily",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/DQ/data_quality_daily.py >> /home/script.master/Documents/Log_files/data_quality_daily.txt 2>&1"
    )

    introducing_broker_eod_download  = BashOperator(
        task_id="introducing_broker_eod_download",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/python_ec2/introducing_broker_eod/eod_download.py >> /home/script.master/Documents/Log_files/eod_download.txt 2>&1"
    )
    
    
with DAG(
    dag_id="Cash_plus_pipeline",
    schedule='0 10 * * *',
    start_date=pendulum.datetime(2023, 12, 5, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["Cash_plus"]
    # default_args={"retries": 5}
):
    client_info_ingestion = BashOperator(
        task_id="client_info_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/BP_related/Tradeup_Cash_plus/client_info_ingestion.py >> /home/script.master/Documents/Log_files/client_info_ingestion.txt 2>&1"
    )

    daily_interests_ingestion = BashOperator(
        task_id="daily_interests_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/BP_related/Tradeup_Cash_plus/daily_interests_ingestion.py >> /home/script.master/Documents/Log_files/daily_interests_ingestion.txt 2>&1"
    )
    
    cusbal_validation = BashOperator(
        task_id="cusbal_validation",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/BP_related/Tradeup_Cash_plus/cusbal_validation.py >> /home/script.master/Documents/Log_files/cusbal_validation.txt 2>&1"
    )

    cusbal_validation.set_downstream(client_info_ingestion)
    client_info_ingestion.set_downstream(daily_interests_ingestion)

with DAG(
    dag_id="fpl_reports_pipeline",
    schedule='50 15 * * 1-5',
    start_date=pendulum.datetime(2023, 12, 5, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["fpl_reports"]
    # default_args={"retries": 5}
):
    fpl_reports_download = BashOperator(
        task_id="fpl_reports_download",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/fpl_reports_download.py >> /home/script.master/Documents/Log_files/fpl_reports_download.txt 2>&1"
    )

    fpl_reports_parsing_ingestion = BashOperator(
        task_id="fpl_reports_parsing_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/fpl_reports_parsing_ingestion.py >> /home/script.master/Documents/Log_files/fpl_reports_parsing_ingestion.txt 2>&1"
    )
    
    fpl_17a4_transmission = BashOperator(
        task_id="fpl_17a4_transmission",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/fpl_17a4_transmission.py >> /home/script.master/Documents/Log_files/fpl_17a4_transmission.txt 2>&1"
    )

    fpl_reports_download.set_downstream(fpl_reports_parsing_ingestion)
    fpl_reports_parsing_ingestion.set_downstream(fpl_17a4_transmission)


with DAG(
    dag_id="fpl_eod_pipeline",
    schedule='45 16 * * 1-5',
    start_date=pendulum.datetime(2023, 12, 5, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["fpl_eod"]
    # default_args={"retries": 5}
):
    fpl_rate_ingestion = BashOperator(
        task_id="fpl_rate_ingestion",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/FPL/fpl_rate_ingestion.py >> /home/script.master/Documents/Log_files/fpl_rate_ingestion.txt 2>&1"
    )

with DAG(
    dag_id="finoptsys_pipeline",
    schedule='45 9 * * 2-6',
    start_date=pendulum.datetime(2024,8, 20, tz='America/Toronto'),
    catchup=False,
    max_active_tasks=1,
    tags=["finoptsys"]
    # default_args={"retries": 5}
):
    finoptsys_pipeline = BashOperator(
        task_id="finoptsys_pipeline",
        bash_command="/home/script.master/pyenv/PROD-Legacy/bin/python /home/script.master/Documents/Python_scripts/SFTP/Finoptsys_SFTP.py >> /home/script.master/Documents/Log_files/Finoptsys_SFTP.txt 2>&1"
    )