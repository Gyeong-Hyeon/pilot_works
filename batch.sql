with temp_web as(
	select anonymous_id, received_at, event
	from tumblbug_web.tracks
	union
	select anonymous_id, received_at, event
	from tum_clickstream.track
)
, temp_track as(
select gw.anonymous_id, gw.received_at, gw.web_event, ga.aos_event, gi.ios_event from(
	select w.anonymous_id, min(w.received_at) as received_at, count(w.event) as web_event
	from temp_web as w
	group by w.anonymous_id
)as gw
full outer join (
	select a.anonymous_id, min(a.received_at) as received_at, count(a.event) as aos_event
	from tumblbug_aos.tracks as a
	group by a.anonymous_id
)as ga
on gw.anonymous_id = ga.anonymous_id 
full outer join (
	select i.anonymous_id, min(i.received_at) as received_at, count(i.event) as ios_event
	from tumblbug_ios.tracks as i
	group by i.anonymous_id
)as gi
on gw.anonymous_id = gi.anonymous_id
)
select t.anonymous_id
	, t.received_at
	, nvl2(t.web_event, 1, 0) as web_visit
	, nvl(t.web_event, 0) as web_event
	, nvl2(t.aos_event, 1, 0) as aos_visit
	, nvl(t.aos_event, 0) as aos_event
	, nvl2(t.ios_event, 1, 0) as ios_visit
	, nvl(t.ios_event, 0) as ios_event
	, (web_visit + aos_visit + ios_visit) as total_visit
	, (t.web_event + t.aos_event + t.ios_event) as total_event
from temp_track as t; --104,906,618.44

/************************************************************************************************
************************************************************************************************/

with temp_web_track as(
	select anonymous_id, received_at, event
	from tumblbug_web.tracks
	union all
	select anonymous_id, received_at, event
	from tum_clickstream.track
)
, temp_web as(
	select wt.anonymous_id, min(wt.received_at) as received_at, count(wt.event) as web_event
	from temp_web_track as wt
	group by wt.anonymous_id
	havning count(*) > 0
)
, temp_aos as (
	select a.anonymous_id, min(a.received_at) as received_at, count(a.event) as aos_event
	from tumblbug_aos.tracks as a
	group by a.anonymous_id
	having count(*) > 0
)
, temp_ios as (
	select i.anonymous_id, min(i.received_at) as received_at, count(i.event) as ios_event
	from tumblbug_ios.tracks as i
	group by i.anonymous_id
	having count(*) > 0
)
select w.anonymous_id
	, w.received_at
	, nvl2(w.web_event, 1, 0) as web_visit
	, nvl(w.web_event, 0) as web_event
	, nvl2(a.aos_event, 1, 0) as aos_visit
	, nvl(a.aos_event, 0) as aos_event
	, nvl2(i.ios_event, 1, 0) as ios_visit
	, nvl(i.ios_event, 0) as ios_event
	, (web_visit + aos_visit + ios_visit) as total_visit
	, (w.web_event + a.aos_event + i.ios_event) as total_event
from temp_web as w
full outer join temp_aos as a
on w.anonymous_id = a.anonymous_id
full outer join temp_ios as i
on w.anonymous_id = i.anonymous_id --907,837,46.01

/************************************************************************************************
************************************************************************************************/

with temp_track as(
	select anonymous_id, received_at, 1 as web_event, 0 as aos_event, 0 as ios_event
	from tumblbug_web.tracks
	where anonymous_id is not null

	union all

	select anonymous_id, received_at, 1 as web_event, 0 as aos_event, 0 as ios_event
	from tum_clickstream.track
	where anonymous_id is not null

	union all

	select anonymous_id, received_at, 0 as web_event, 1 as aos_event, 0 as ios_event
	from tumblbug_aos.tracks
	where anonymous_id is not null

	union all

	select anonymous_id, received_at, 0 as web_event, 0 as aos_event, 1 as ios_event
	from tumblbug_ios.tracks
	where anonymous_id is not null
)
select t.anonymous_id
	, t.received_at as event_at
	, max(t.web_event) as web_visit
	, count(t.web_event) as web_event
	, max(t.aos_event) as aos_visit
	, count(t.aos_event) as aos_event
	, max(t.ios_event) as ios_visit
	, count(t.ios_event) as ios_event
	, max(values (web_visit), (aos_visit), (ios_visit)) as total_visit
	, (max(web_event) + max(aos_event) + max(ios_event)) as total_event
from temp_track as t
group by 1,2;

--16,560,628.92

/************************************************************************************************
************************************************************************************************/

with temp_track as(
	select anonymous_id, received_at, 1 as web_event, 0 as aos_event, 0 as ios_event
	from tumblbug_web.tracks
	group by 1,2
	having count(*) > 0
	union all
	select anonymous_id, received_at, 1 as web_event, 0 as aos_event, 0 as ios_event
	from tum_clickstream.track
	group by 1,2
	having count(*) > 0	
	union all
	select anonymous_id, received_at, 0 as web_event, 1 as aos_event, 0 as ios_event
	from tumblbug_aos.tracks
	group by 1,2
	having count(*) > 0
	union all
	select anonymous_id, received_at, 0 as web_event, 0 as aos_event, 1 as ios_event
	from tumblbug_ios.tracks
	group by 1,2
	having count(*) > 0
)
select anonymous_id
	, received_at as event_at
	, max(web_event) as web_visit
	, count(web_event) as web_event
	, max(aos_event) as aos_visit
	, count(aos_event) as aos_event
	, max(ios_event) as ios_visit
	, count(ios_event) as ios_event
	, decode((web_visit + aos_visit + ios_visit), 0, 0, 1) as total_visit
	, (max(web_event) + max(aos_event) + max(ios_event)) as total_event
from temp_track
group by 1,2;

/************************************************************************************************
************************************************************************************************/

with temp_web as(
	select anonymous_id, event_at, sum(web_event) as web_event
	from(
		select anonymous_id, date_trunc('day', "timestamp" + interval '9 hour') as event_at, count(*) as web_event
		from tumblbug_web.tracks
		where anonymous_id is not null 
		group by 1,2
		having count(*) > 0
		union all
		select anonymous_id, date_trunc('day', received_at + interval '9 hour'), count(*) as web_event
		from tum_clickstream.track
		where anonymous_id is not null
		group by 1,2
		having count(*) > 0
	)
	group by 1,2
)
, temp_aos as(
	select anonymous_id, date_trunc('day', "timestamp" + interval '9 hour') as event_at, count(*) as aos_event
	from tumblbug_aos.tracks
	where anonymous_id is not null
	group by 1,2
	having count(*) > 0
)
, temp_ios as(
	select anonymous_id, date_trunc('day', "timestamp" + interval '9 hour') as event_at, count(*) as ios_event
	from tumblbug_ios.tracks
	where anonymous_id is not null
	group by 1,2
	having count(*) > 0
)
select anonymous_id
	, event_at
	, decode(max(web_event), 0, 0, 1) as web_visit
	, max(web_event) as web_event
	, decode(max(aos_event), 0, 0, 1) as aos_visit
	, max(aos_event) as aos_event
	, decode(max(ios_event), 0, 0, 1) as ios_visit
	, max(ios_event) as ios_event
	, decode((web_visit + aos_visit + ios_visit), 0, 0, 1) as total_visit
	, (max(web_event) + max(aos_event) + max(ios_event)) as total_event
from (
	select anonymous_id, event_at, web_event, 0 as aos_event, 0 as ios_event
	from temp_web
	union all
	select anonymous_id, event_at, 0 as web_event, aos_event, 0 as ios_event
	from temp_aos
	union all
	select anonymous_id, event_at, 0 as web_event, 0 as aos_event, ios_event
	from temp_ios
)
group by 1,2;
