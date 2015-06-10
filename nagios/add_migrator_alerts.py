import sys

class Alerts:
	  time = '"15min"'
          def __init__(self, topic_name, warning=1000000, critical=3000000, dc="*", env="*"):
                     self.warning = warning
                     self.critical = critical
                     self.topic_name = topic_name
                     self.dc = dc
                     self.env = env
          
          def set_topic_alert(self,file_ptr):
                     entry = "graphite.absolute_threshold('movingAverage(maxSeries(stats." + self.dc + ".migrator." + self.env + "." + self.topic_name + ".kafka_offset_lag.*)," + Alerts.time + ")', alias='migrator_lag_alerts_" + self.topic_name + "', warning_over=" + str(self.warning) + ", critical_over=" + str(self.critical) + ")"
                     print >> file_ptr, entry


alertList = [
          Alerts("api_client_signups",),
          Alerts("api_client_transactions",),
          Alerts("api_created_trips",),
          Alerts("api_db_nearestcab_logs_client",),
          Alerts("api_driver_signups",),
          Alerts("api_driver_status_change",),
          Alerts("api_goldeta_accuracy",),
          Alerts("api_lib_trips_trip_updator",),
          Alerts("api_promotions_apply",),
          Alerts("cerebro_geosurge_recommended_multipliers_v2",),
          Alerts("cleopatra_artemis_data_email",),
          Alerts("cleopatra_artemis_data_sms",),
          Alerts("cream_transactions",),
          Alerts("dispatch_events",),
          Alerts("dispatch_events_client_canceled",),
          Alerts("dispatch_events_client_eyeballed",),
          Alerts("dispatch_events_destination_set",),
          Alerts("dispatch_events_driver_accepted",),
          Alerts("dispatch_events_driver_arrived",),
          Alerts("dispatch_events_driver_canceled",),
          Alerts("dispatch_events_driver_dispatched",),
          Alerts("dispatch_events_driver_expired",),
          Alerts("dispatch_events_driver_rated",),
          Alerts("dispatch_events_driver_rejected",),
          Alerts("dispatch_events_driver_status_changes",),
          Alerts("dispatch_events_fare_split_accepted",),
          Alerts("dispatch_events_fare_split_invited",),
          Alerts("dispatch_events_pickup_requested",),
          Alerts("dispatch_events_ridepool_merged",),
          Alerts("dispatch_events_surge_multipliers_received",),
          Alerts("dispatch_events_trip_began",),
          Alerts("dispatch_events_trip_ended",),
          Alerts("dispatch_events_trip_shared",),
          Alerts("dispatch_events_trip_unfulfilled",),
          Alerts("dispatch_events_trip_sent_to_api",),
          Alerts("dispatch_events_user_session",),
          Alerts("free-candy_rider_signups",),
          Alerts("halyard_events_treatments",),
          Alerts("hp_artemis_query",),
          Alerts("hp_event_user",),
          Alerts("hp_event_user_driver_app",),
          Alerts("hp_uberex_metrics",),
          Alerts("mobile_events",),
          Alerts("money_fraud_creditcard_create_decline",),
          Alerts("nagios_notifications",),
          Alerts("zendesk_tickets_first_filed",),
          Alerts("zendesk_tickets_first_reply",),
          Alerts("zendesk_tickets_first_resolved",),
          Alerts("rt-san_francisco", env="kloak_b"),
          Alerts("rt-london", env="kloak_b"),
          Alerts("rt-new_york", env="kloak_b"),
          Alerts("rt-chicago", env="kloak_b"),
          Alerts("rt-washington_DC", env="kloak_b"), 
         ]

i = 0
part = 0
file_ptr = None
#have only 10 checks per file so that we don't time out
for alert in alertList:
     if(i%10 == 0):
           if(file_ptr!=None):
                 file_ptr.close()
           file_ptr = open("./thresholds/kloak_migrator_lag_alerts_part_"+str(part),"w")  
           part = part + 1
     alert.set_topic_alert(file_ptr)
     i = i+1
