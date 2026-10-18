Treats a missing/negative committed offset (the `-1001` "no commit" sentinel or None) as a literal offset instead of "fully unread", so a never-consumed partition reports the wrong lag.
