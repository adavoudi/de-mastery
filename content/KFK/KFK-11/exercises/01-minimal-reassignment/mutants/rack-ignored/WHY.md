Never enforces rack distinctness, so multiple replicas of a partition can land on one rack — a single rack/AZ outage then takes the partition offline.
