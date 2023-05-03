from datetime import datetime
import boto3
import os

# Replace these values with yours
my_region= os.environ['REGION']
my_kda_appname = os.environ['KDA_APP_NAME']

# Replace these with values pertinent
# to your scenario
high_scale_start_hour = os.environ['SCALE_UP_HOUR'] # 5 am in PST
high_scale_end_hour = os.environ['SCALE_DOWN_HOUR'] # 10 am in PST
low_scale_kpu_count =os.environ['LOW_KPU']
high_scale_kpu_count = os.environ['HIGH_KPU']

# The main lambda handler
def handler(event, context):
    try:
        check_and_perform_scaling()
        return True
    except Exception as e:
        print("Failed to scale due to an exception!")
        print(e)
        return False

def check_and_perform_scaling():
    # IMPORTANT: replace with your region
    kda = boto3.client('kinesisanalyticsv2', region_name=my_region)

    status = ""
    current_app_version = -1
    response = kda.describe_application(ApplicationName=my_kda_appname)
    if response and "ApplicationDetail" in response:
        app_detail = response["ApplicationDetail"]
        if "ApplicationStatus" in app_detail:
            status = app_detail["ApplicationStatus"]
            print("App status: " + status)
        else:
            print("Unable to get application status")
            return

        if "ApplicationVersionId" in app_detail:
            current_app_version = app_detail["ApplicationVersionId"]
            print("Current app version: " + str(current_app_version))
        else:
            print("Unable to get current app version")
            return

    if not status:
        print("Unable to get current app status. Not scaling.")
        return

    if current_app_version <= 0:
        print("Unable to get current application version. Not scaling.")
        return

    if status == "RUNNING":
        perform_scaling(app_detail, kda, my_kda_appname, current_app_version)
    else:
        print("Not scaling because app is not running.")
        print("Current status: " + status)

def is_in_high_scale_period():
    current_time = datetime.utcnow()
    current_hour = current_time.hour
    return current_hour >= high_scale_start_hour and current_hour <= high_scale_end_hour

def perform_scaling(app_detail, kda_client, kda_appname, current_app_version):
    app_config = app_detail["ApplicationConfigurationDescription"]
    flink_app_config = app_config["FlinkApplicationConfigurationDescription"]
    parallelism_config = flink_app_config["ParallelismConfigurationDescription"]
    parallelism = parallelism_config["Parallelism"]
    current_parallelism = parallelism_config["CurrentParallelism"]
    if is_in_high_scale_period():
        if current_parallelism != high_scale_kpu_count:
            scale_app(kda_client, kda_appname, current_app_version, high_scale_kpu_count)
        else:
            print("Not scaling app because already at high scale kpu count: " + str(high_scale_kpu_count))
    else:
        if current_parallelism != low_scale_kpu_count:
            scale_app(kda_client, kda_appname, current_app_version, low_scale_kpu_count)
        else:
            print("Not scaling app because already at low scale kpu count: " + str(low_scale_kpu_count))

def scale_app(kda_client, kda_appname, current_app_version, kpu_count):
    print("Scaling app to: " + str(kpu_count))
    update_config = {
        'FlinkApplicationConfigurationUpdate': {
            'ParallelismConfigurationUpdate': {
                'ConfigurationTypeUpdate': 'CUSTOM',
                'ParallelismUpdate': kpu_count,
                'ParallelismPerKPUUpdate': 1, # we assume this is always 1
                'AutoScalingEnabledUpdate': False,
            }
        }
    }

    response = kda_client.update_application(
        ApplicationName=kda_appname,
        CurrentApplicationVersionId=current_app_version,
        ApplicationConfigurationUpdate=update_config,
    )

    print("Updated application parallelism. See response below.")
    print(response)


if __name__ == "__main__":
    lambda_handler(None, None)