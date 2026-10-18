Uses leftJoin, so an order whose customer isn't in the table yet emits "null|amount" instead of nothing — violating inner KStream-KTable semantics.
