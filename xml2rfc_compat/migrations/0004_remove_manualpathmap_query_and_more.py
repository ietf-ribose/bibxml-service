# Generated by Django 4.0.2 on 2022-02-08 15:51

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('xml2rfc_compat', '0003_manualpathmap_docid'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='manualpathmap',
            name='query',
        ),
        migrations.RemoveField(
            model_name='manualpathmap',
            name='query_format',
        ),
    ]