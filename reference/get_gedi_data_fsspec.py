
import sys
import h5py
import boto3
import botocore
import fsspec
import requests
from maap.maap import MAAP
maap = MAAP(maap_host="api.maap-project.org")
import os


def assume_role_credentials(ssm_parameter_name):
    # Create a session using your current credentials
    session = boto3.Session()

    # Retrieve the SSM parameter
    ssm = session.client('ssm', "us-west-2")
    parameter = ssm.get_parameter(
        Name=ssm_parameter_name,
        WithDecryption=True
    )
    parameter_value = parameter['Parameter']['Value']

    # Assume the DAAC access role
    sts = session.client('sts')
    assumed_role_object = sts.assume_role(
        RoleArn=parameter_value,
        RoleSessionName='TutorialSession'
    )

    # From the response that contains the assumed role, get the temporary
    # credentials that can be used to make subsequent API calls
    credentials = assumed_role_object['Credentials']

    return credentials

# We can pass assumed role credentials into fsspec
def fsspec_access(credentials):
    return fsspec.filesystem(
        "s3",
        key=credentials['AccessKeyId'],
        secret=credentials['SecretAccessKey'],
        token=credentials['SessionToken']
    )

def lpdaac_gedi_https_to_s3(url):
    dir_comps = url.split("/")
    return f"s3://lp-prod-protected/{dir_comps[6]}/{dir_comps[8].strip('.h5')}/{dir_comps[8]}"

def get_gedi_data(url):
    s3_fsspec = fsspec_access(assume_role_credentials("/iam/maap-data-reader"))
    basename = os.path.basename(url)
    outfp = f"output/{basename}"
    gedi_ds = h5py.File(s3_fsspec.open(lpdaac_gedi_https_to_s3(url)), "r")
    with h5py.File(outfp, 'w') as dst:
        for obj in gedi_ds.keys():        
            gedi_ds.copy(obj, dst)   
    gedi_ds.close()
    # Return filepath!
    return outfp
