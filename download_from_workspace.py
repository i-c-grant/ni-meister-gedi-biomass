import os
import json
import subprocess
import click

@click.command()
@click.option('--credentials', '-c', required=True, type=str,
              help='Path to JSON file containing AWS credentials')
@click.option('--output-dir', '-o', required=True, type=str,
              help='Local directory to download files to')
@click.option('--algorithm', '-a', required=True, type=str,
              help='Algorithm name')
@click.option('--version', '-v', required=True, type=str,
              help='Algorithm version')
@click.option('--tag', '-t', required=True, type=str,
              help='Job tag')
def main(credentials: str, output_dir: str, algorithm: str, version: str, tag: str):
    """Download results from MAAP workspace S3 bucket."""
    
    # Load AWS credentials
    with open(credentials) as f:
        creds = json.load(f)
    
    # Map lowercase keys to AWS environment variable names
    aws_env = os.environ.copy()
    aws_env.update({
        'AWS_ACCESS_KEY_ID': creds['aws_access_key_id'],
        'AWS_SECRET_ACCESS_KEY': creds['aws_secret_access_key'],
        'AWS_SESSION_TOKEN': creds['aws_session_token']
    })

    # Construct S3 URL
    s3_url = f's3://maap-ops-workspace/iangrant94/dps_output/{algorithm}/{version}/{tag}/'

    # Run AWS sync command in environment with credentials
    subprocess.run([
        'aws', 's3', 'sync',
        s3_url,
        output_dir,
        '--exclude', '*',
        '--include', '*.bz2'
    ], env=aws_env)
    
    print('Download complete')

if __name__ == '__main__':
    main()
