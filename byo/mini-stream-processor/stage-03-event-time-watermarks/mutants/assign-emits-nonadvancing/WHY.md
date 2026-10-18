assign_watermarks emits a watermark after EVERY event, even when it did not advance -> non-monotone (duplicate) watermarks.
