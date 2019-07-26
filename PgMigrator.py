import PostgresHandler as pg
import configs.ConfigHandler as config
default_data = config.get_conf('config.ini','DefaultData')

con = None

# CSDB_CacheUpdates ignore

def CSDB_CallCDRProcessed(): #tested
    try:
        print("CSDB_CallCDRProcesseds - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_CallCDRProcesseds" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("CSDB_CallCDRProcesseds - BusinessUnit 'ADD' process completed")
        print("CSDB_CallCDRProcesseds - QueuePriority 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_CallCDRProcesseds" ADD COLUMN IF NOT EXISTS "QueuePriority" integer;""")
        print("CSDB_CallCDRProcesseds - QueuePriority 'ADD' process completed")
        print("CSDB_CallCDRProcesseds - TimeAfterInitialBridge 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_CallCDRProcesseds" ADD COLUMN IF NOT EXISTS "TimeAfterInitialBridge" integer;""")
        print("CSDB_CallCDRProcesseds - TimeAfterInitialBridge 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("CSDB_CallCDRProcesseds - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."CSDB_CallCDRProcesseds" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("CSDB_CallCDRProcesseds - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

    # no need to update old data

def CSDB_CallCDRs():
    try:
        print("CSDB_CallCDRs - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_CallCDRs" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("CSDB_CallCDRs - BusinessUnit 'ADD' process completed")
        print("CSDB_CallCDRs - MemberUuid 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_CallCDRs" ADD COLUMN IF NOT EXISTS "MemberUuid" varchar(255);""")
        print("CSDB_CallCDRs - MemberUuid 'ADD' process completed")
        print("CSDB_CallCDRs - QueuePriority 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_CallCDRs" ADD COLUMN IF NOT EXISTS "QueuePriority" integer ;""")
        print("CSDB_CallCDRs - QueuePriority 'ADD' process completed")
        print("CSDB_CallCDRs - TimeAfterInitialBridge 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_CallCDRs" ADD COLUMN IF NOT EXISTS "TimeAfterInitialBridge" integer ;""")
        print("CSDB_CallCDRs - TimeAfterInitialBridge 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("CSDB_CallCDRs - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."CSDB_CallCDRs" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("CSDB_CallCDRs - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

    # no need to update old data

def CSDB_CallRules(): # tested

    try:
        print("CSDB_CallRules - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_CallRules" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("CSDB_CallRules - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("CSDB_CallRules - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."CSDB_CallRules" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("CSDB_CallRules - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

def CSDB_CallServers(): #Imp should manually add values
    try:
        print("CSDB_CallServers - MainIp 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_CallServers" ADD COLUMN IF NOT EXISTS "MainIp" varchar(255);""")
        print("CSDB_CallServers - MainIp 'ADD' process completed")
    except Exception as err:
        print(err)

    # try:
    #     con.execute_query("""UPDATE "public"."CSDB_CallServers" SET "MainIp" = '?' """)
    # except Exception as err:
    #     print(err)


def CSDB_Crons():  # data is not persistent so no need to migration
    try:
        print("CSDB_Crons - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_Crons" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("CSDB_Crons - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("CSDB_Crons - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."CSDB_Crons" SET "Timezone" = '?' """)
        print("CSDB_Crons - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

def CSDB_DVPEvents(): # tested
    try:
        print("CSDB_DVPEvents - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_DVPEvents" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("CSDB_DVPEvents - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("CSDB_DVPEvents - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."CSDB_DVPEvents" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("CSDB_DVPEvents - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

def CSDB_FileCategories(): # tested
    try:
        print("CSDB_FileCategories - Company 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_FileCategories" ADD COLUMN IF NOT EXISTS "Company" integer;""")
        print("CSDB_FileCategories - Company 'ADD' process completed")
        print("CSDB_FileCategories - Tenant 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_FileCategories" ADD COLUMN IF NOT EXISTS "Tenant" integer;""")
        print("CSDB_FileCategories - Tenant 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("CSDB_FileCategories - Company, Tenant 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."CSDB_FileCategories" SET "Company" = {0}, "Tenant" = {1};""".format(default_data['company'], default_data['tenant']))
        print("CSDB_FileCategories - Company, Tenant 'UPDATE' process completed")
    except Exception as err:
        print(err)

def CSDB_QueueProfiles(): # tested dialtime, maxwaitqueuethreshold check at singer. In development server lowercased fields are available in addition to them.
    try:
        print("CSDB_QueueProfiles - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_QueueProfiles" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("CSDB_QueueProfiles - BusinessUnit 'ADD' process completed")
        # print("CSDB_QueueProfiles - dialtime 'ADD' process started")
        # con.execute_query("""ALTER TABLE "public"."CSDB_QueueProfiles" ADD COLUMN IF NOT EXISTS "dialtime" integer;""")
        # print("CSDB_QueueProfiles - dialtime 'ADD' process completed")
        # print("CSDB_QueueProfiles - maxwaitqueuethreshold 'ADD' process started")
        # con.execute_query("""ALTER TABLE "public"."CSDB_QueueProfiles" ADD COLUMN IF NOT EXISTS "maxwaitqueuethreshold" integer;""")
        # print("CSDB_QueueProfiles - maxwaitqueuethreshold 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("CSDB_FileCategories - BusinessUnit 'ADD' process started")
        con.execute_query("""UPDATE "public"."CSDB_QueueProfiles" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("CSDB_FileCategories - BusinessUnit 'ADD' process completed")
        # print("CSDB_FileCategories - BusinessUnit 'ADD' process started")
        # con.execute_query("""UPDATE "public"."CSDB_QueueProfiles" SET "dialtime" = '{0}'""".format(default_data['bu']))
        # print("CSDB_FileCategories - BusinessUnit 'ADD' process completed")
        # print("CSDB_FileCategories - BusinessUnit 'ADD' process started")
        # con.execute_query("""UPDATE "public"."CSDB_QueueProfiles" SET "maxwaitqueuethreshold" = '{0}'""".format(default_data['bu']))
        # print("CSDB_FileCategories - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

# CSDB_Trunks manual migration
def CSDB_Trunks():
    try:
        print("CSDB_Trunks - Register 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."CSDB_Trunks" ADD COLUMN IF NOT EXISTS "Register" boolean;""")
        print("CSDB_Trunks - Register 'ADD' process completed")
    except Exception as err:
        print(err)

def DB_AuditTrails(): # tested
    try:
        print("DB_AuditTrails - OtherData 'RENAME' process started")
        con.execute_query("""ALTER TABLE "public"."DB_AuditTrails" RENAME COLUMN "OtherData" TO "OtherJsonData";""")
        print("DB_AuditTrails - OtherData 'RENAME' process completed")
        print("DB_AuditTrails - json 'TYPE' process started")
        con.execute_query("""ALTER TABLE "public"."DB_AuditTrails" ALTER COLUMN "OtherJsonData"  TYPE json USING "OtherJsonData"::json;""")
        print("DB_AuditTrails - json 'TYPE' process completed")
    except Exception as err:
        print(err)

    # no existing data at Singer so nothing to update

def DB_CAMP_CallBackReasons(): # tested
    try:
        print("DB_CAMP_CallBackReasons - CompanyId 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_CAMP_CallBackReasons" DROP COLUMN IF EXISTS "CompanyId";""")
        print("DB_CAMP_CallBackReasons - CompanyId 'ADD' process completed")
        print("DB_CAMP_CallBackReasons - TenantId 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_CAMP_CallBackReasons" DROP COLUMN IF EXISTS "TenantId";""")
        print("DB_CAMP_CallBackReasons - TenantId 'ADD' process completed")
    except Exception as err:
        print(err)

    # no data to update

def DB_CAMP_CallbackInfos():
    try:
        print("DB_CAMP_CallbackInfos - CampaignName 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_CAMP_CallbackInfos" ADD COLUMN IF NOT EXISTS "CampaignName" varchar(255);""")
        print("DB_CAMP_CallbackInfos - CampaignName 'ADD' process completed")
    except Exception as err:
        print(err)

    # no data to update

def DB_CAMP_CampDialoutInfos():
    try:
        print("DB_CAMP_CampDialoutInfos - CampaignName, DialNumber,  'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_CAMP_CampDialoutInfos" ADD COLUMN IF NOT EXISTS "CampaignName" varchar(255);""")
        con.execute_query("""ALTER TABLE "public"."DB_CAMP_CampDialoutInfos" ADD COLUMN IF NOT EXISTS "DialNumber" varchar(255);""")
        print("DB_CAMP_CampDialoutInfos - CampaignName, DialNumber,  'ADD' process completed")
    except Exception as err:
        print(err)
        # no data to update


def DB_CAMP_Configurations():
    try:
        print("DB_CAMP_CampDialoutInfos - DuplicateNumTimeout, IntegrationData, NumberLoadingMethod 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_CAMP_Configurations" ADD COLUMN IF NOT EXISTS "DuplicateNumTimeout" integer;""")
        con.execute_query("""ALTER TABLE "public"."DB_CAMP_Configurations" ADD COLUMN IF NOT EXISTS "IntegrationData" json;""")
        con.execute_query("""ALTER TABLE "public"."DB_CAMP_Configurations" ADD COLUMN IF NOT EXISTS "NumberLoadingMethod" varchar(255);""")
        print("DB_CAMP_CampDialoutInfos - DuplicateNumTimeout, IntegrationData, NumberLoadingMethod 'ADD' process completed")
    except Exception as err:
        print(err)

    # no data to update

def DB_RES_ResourceAcwInfos():
    try:
        print("DB_RES_ResourceAcwInfos - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_RES_ResourceAcwInfos" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("DB_RES_ResourceAcwInfos - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("DB_RES_ResourceAcwInfos - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."DB_RES_ResourceAcwInfos" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("DB_RES_ResourceAcwInfos - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

def DB_RES_ResourceStatusChangeInfos(): # tested
    try:
        print("DB_RES_ResourceStatusChangeInfos - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_RES_ResourceStatusChangeInfos" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("DB_RES_ResourceStatusChangeInfos - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("DB_RES_ResourceStatusChangeInfos - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."DB_RES_ResourceStatusChangeInfos" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("DB_RES_ResourceStatusChangeInfos - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

def DB_RES_ResourceStatusDurationInfos(): # tested
    try:
        print("DB_RES_ResourceStatusDurationInfos - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_RES_ResourceStatusDurationInfos" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("DB_RES_ResourceStatusDurationInfos - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("DB_RES_ResourceStatusDurationInfos - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."DB_RES_ResourceStatusDurationInfos" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("DB_RES_ResourceStatusDurationInfos - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

def DB_RES_ResourceTaskRejectInfos(): # tested
    try:
        print("DB_RES_ResourceTaskRejectInfos - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_RES_ResourceTaskRejectInfos" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("DB_RES_ResourceTaskRejectInfos - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("DB_RES_ResourceTaskRejectInfos - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."DB_RES_ResourceTaskRejectInfos" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("DB_RES_ResourceTaskRejectInfos - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

def DB_VOX_OderData(): # tested
    try:
        print("DB_VOX_OderData - ChannelCount, VoxStatus 'ADD' and 'DROP' process started")
        con.execute_query("""ALTER TABLE "public"."DB_VOX_OderData" ADD COLUMN IF NOT EXISTS "ChannelCount" integer;""")
        con.execute_query("""ALTER TABLE "public"."DB_VOX_OderData" DROP COLUMN IF EXISTS "VoxStatus";""")
        print("DB_VOX_OderData - ChannelCount, VoxStatus 'ADD' and 'DROP' process completed")
    except Exception as err:
        print(err)

def DB_WalletHistories(): # tested
    try:
        print("DB_WalletHistories - SessionID 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."DB_WalletHistories" ADD COLUMN IF NOT EXISTS "SessionID" varchar(255);""")
        print("DB_WalletHistories - SessionID 'ADD' process completed")
    except Exception as err:
        print(err)

def Dashboard_DailySummaries():
    try:
        print("Dashboard_DailySummaries - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."Dashboard_DailySummaries" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("Dashboard_DailySummaries - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("Dashboard_DailySummaries - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."Dashboard_DailySummaries" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("Dashboard_DailySummaries - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

def Dashboard_ThresholdBreakDowns():
    try:
        print("Dashboard_ThresholdBreakDowns - BusinessUnit 'ADD' process started")
        con.execute_query("""ALTER TABLE "public"."Dashboard_ThresholdBreakDowns" ADD COLUMN IF NOT EXISTS "BusinessUnit" varchar(255);""")
        print("Dashboard_ThresholdBreakDowns - BusinessUnit 'ADD' process completed")
    except Exception as err:
        print(err)

    try:
        print("Dashboard_ThresholdBreakDowns - BusinessUnit 'UPDATE' process started")
        con.execute_query("""UPDATE "public"."Dashboard_ThresholdBreakDowns" SET "BusinessUnit" = '{0}'""".format(default_data['bu']))
        print("Dashboard_ThresholdBreakDowns - BusinessUnit 'UPDATE' process completed")
    except Exception as err:
        print(err)

if __name__ == "__main__":
    con = pg.PostgresHandler('db2')
    con.initiate()
    # CSDB_CallCDRProcessed()
    # CSDB_CallRules()
    # CSDB_DVPEvents()
    # CSDB_FileCategories()
    # DB_AuditTrails()
    # DB_CAMP_CallBackReasons()
    # DB_CAMP_CallbackInfos()
    # DB_CAMP_CampDialoutInfos()
    # DB_CAMP_Configurations()
    # DB_RES_ResourceAcwInfos()
    # DB_RES_ResourceStatusChangeInfos()
    # DB_RES_ResourceStatusDurationInfos()
    # DB_RES_ResourceTaskRejectInfos()
    # DB_VOX_OderData()
    # DB_WalletHistories()
    # Dashboard_DailySummaries()
    # Dashboard_ThresholdBreakDowns()
    # CSDB_QueueProfiles()
    # CSDB_CallCDRs()
    # CSDB_CallServers()
    # CSDB_Trunks()
