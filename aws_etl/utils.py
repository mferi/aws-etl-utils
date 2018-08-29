# -*- coding: utf-8 -*-

"""Utilities for AWS ETL."""

import base64
from dateutil.relativedelta import relativedelta
from datetime import date
import json
import logging
import os
import urllib

import boto3

DEFAULT_REGION = 'eu-west-1'


log = logging.getLogger(__name__)


def decrypt(enc_string, region_name=None):
    """Given a KMS encrypted string decrypt it."""
    if not region_name and 'DEFAULT_REGION' not in os.environ:
        set_region_name()
        region_name = os.environ['DEFAULT_REGION']
    kms = boto3.client('kms', region_name=region_name)
    return kms.decrypt(CiphertextBlob=base64.b64decode(enc_string))['PlainText']


def encrypt(string, key_id, region_name=None):
    """Encrypt given a KMS key."""
    if len(string) > 4096:
        log.error("String to encrypt can't be longer than 4KB")
        raise ValueError("Text to encrypt can't be longer than 4KB")
    if not region_name and 'DEFAULT_REGION' not in os.environ:
        region_name = get_instance_region()
    kms = boto3.client('kms', region_name=region_name)
    enc_string = kms.encrypt(Plaintext=string, KeyId=key_id)['CiphertextBlob']
    return base64.b64encode(enc_string)


def get_parameter(parameter_name, decrypted=True):
    """Get a parameter stored on SSM."""
    ssm = boto3.client('ssm')
    return ssm.get_parameter(Name=parameter_name, WithDecryption=decrypted)


def get_instance_region():
    """Return the AWS region of the instance."""
    url = 'http://169.254.169.254/latest/dynamic/instance-identity/document'
    try:
        with urllib.request.urlopen(url) as response:
            region_name = json.loads(response.read())['region']
        return region_name
    except urllib.error.URLError as ex:
        log.error('Could not retrieve the region name: %s', ex)
        return None


def set_region_name():
    """Ensure the default region is set or set default one."""
    if 'DEFAULT_REGION' not in os.environ:
        region_name = boto3.Session().region_name
        if not region_name:
            region_name = get_instance_region()
            if not region_name:
                region_name = DEFAULT_REGION
        log.info('Setting region name as %s', region_name)
        os.environ['DEFAULT_REGION'] = region_name
    else:
        log.info('Region name already set as %s', os.environ['DEFAULT_REGION'])


def default_parameters():
    """App default parameters are:
    lw: last week year
    lwy: last week year
    """
    # ADD HERE OTHER PARAMETERS AS REQUIRED BY APP
    last_week = date.today() - relativedelta(days=+7)
    last_week_num = last_week.isocalendar()[1]
    log.debug('Last week number: {}'.format(last_week_num))
    last_week_year_num = last_week.isocalendar()[0]
    log.debug('Last week year: {}'.format(last_week_year_num))
    # Update these parameters as required by project basis
    return \
        {
            'lw': last_week_num,
            'lwy': last_week_year_num,
        }
