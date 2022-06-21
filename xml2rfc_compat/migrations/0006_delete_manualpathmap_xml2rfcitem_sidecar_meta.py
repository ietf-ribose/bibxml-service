# Generated by Django 4.0.5 on 2022-06-18 07:29

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('xml2rfc_compat', '0005_alter_manualpathmap_xml2rfc_subpath'),
    ]

    operations = [
        migrations.DeleteModel(
            name='ManualPathMap',
        ),
        migrations.AddField(
            model_name='xml2rfcitem',
            name='sidecar_meta',
            field=models.JSONField(default=dict),
            preserve_default=False,
        ),
    ]