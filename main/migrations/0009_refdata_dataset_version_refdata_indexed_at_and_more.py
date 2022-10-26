# Generated by Django 4.0 on 2022-10-24 18:45

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('main', '0008_refdata_latest_date'),
    ]

    operations = [
        migrations.AddField(
            model_name='refdata',
            name='dataset_version',
            field=models.CharField(db_index=True, default='HEAD', max_length=128),
        ),
        migrations.AddField(
            model_name='refdata',
            name='indexed_at',
            field=models.DateTimeField(default=django.utils.timezone.now),
        ),
        migrations.AddField(
            model_name='refdata',
            name='indexing_outcome',
            field=models.JSONField(default=dict),
        ),
    ]