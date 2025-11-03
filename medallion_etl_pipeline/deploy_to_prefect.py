# =================================================================
# deploy_to_prefect.py
"""
Deploy pipeline to Prefect Cloud/Server
"""

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from main_pipeline import medallion_pipeline

def deploy():
    """Deploy pipeline to Prefect"""
    
    # Create deployment
    deployment = Deployment.build_from_flow(
        flow=medallion_pipeline,
        name="medallion-etl-production",
        version="1.0.0",
        work_queue_name="etl-queue",
        schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),  # Run daily at 2 AM
        parameters={},
        tags=["etl", "medallion", "production"]
    )
    
    # Apply deployment
    deployment.apply()
    
    print("✅ Deployment created successfully!")
    print("📋 Deployment name: medallion-etl-production")
    print("⏰ Schedule: Daily at 2 AM UTC")
    print("🔧 To run manually: prefect deployment run 'Medallion Architecture ETL Pipeline/medallion-etl-production'")


if __name__ == "__main__":
    deploy()