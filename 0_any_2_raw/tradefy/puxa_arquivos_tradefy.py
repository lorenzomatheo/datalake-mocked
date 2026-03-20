# Databricks notebook source
# TODO: migrar para DatabricksEnvironmentBuilder. Bucket S3 e credenciais SFTP estão hardcoded.
# MAGIC %pip install paramiko boto3

# COMMAND ----------

import os
import stat

import boto3
import paramiko

# COMMAND ----------

# AWS
s3_bucket_name = "maggu-datalake-prod"
s3_target_folder = "1-raw-layer/tradefy"
sft_password = dbutils.secrets.get(scope="databricks", key="TRADEFY_SFTP_PASSWORD")

# Credencias sftp
sftp_host = "integracao.tradefy.com.br"
sftp_port = 2222
sftp_user = "maggu"
sftp_pass = sft_password
remote_directory = "/"

# COMMAND ----------

# Initialize a transport object
transport = None
try:
    # 1. Establish connections
    # Connect to S3
    s3 = boto3.client('s3')
    print("✅ Successfully connected to S3.")

    # Connect to SFTP
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_user, password=sftp_pass)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("✅ Successfully connected to the SFTP server.")

    # 2. List files already in the S3 target folder
    print(
        f"🔎 Checking for existing files in s3://{s3_bucket_name}/{s3_target_folder}..."
    )
    s3_files = set()
    # Use a paginator to handle cases with many files
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket_name, Prefix=s3_target_folder)
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                # Extract just the filename from the S3 key
                filename = os.path.basename(obj['Key'])
                if filename:
                    s3_files.add(filename)
    print(f"  -> Found {len(s3_files)} existing files in S3.")

    # 3. List all regular files from the SFTP directory
    sftp.chdir(remote_directory)
    all_sftp_files_attr = sftp.listdir_attr()

    # Create a set of filenames for files on the SFTP server
    sftp_files = {f.filename for f in all_sftp_files_attr if stat.S_ISREG(f.st_mode)}
    print(f"🔎 Found {len(sftp_files)} files in the SFTP directory.")

    # 4. Determine which files are new by comparing the two sets
    new_files_to_upload = sftp_files - s3_files

    # 5. Upload only the new files
    if not new_files_to_upload:
        print("\n✅ No new files to upload. S3 is already up-to-date.")
    else:
        print(
            f"\n⏳ Found {len(new_files_to_upload)} new file(s) to upload: {', '.join(new_files_to_upload)}"
        )
        for file_name in new_files_to_upload:
            s3_key = os.path.join(s3_target_folder, file_name)

            # Stream the file directly from SFTP to S3
            with sftp.open(file_name, 'rb') as f:
                s3.upload_fileobj(f, s3_bucket_name, s3_key)
                print(
                    f"  -> Successfully uploaded '{file_name}' to s3://{s3_bucket_name}/{s3_key}"
                )

    print("\n✅ Task complete.")

finally:
    # 6. Close the SFTP connection
    if 'sftp' in locals() and sftp.get_channel().get_transport().is_active():
        sftp.close()
    if transport and transport.is_active():
        transport.close()
    print("\n🔌 SFTP Connection closed.")
