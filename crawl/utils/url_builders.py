from urllib.parse import quote
import random

def _slug_city(city: str) -> str:
    return city.replace(" ", "-")

def build_zillow_urls(zips, cities_states, seeds):
    urls = []
    for z in zips:
        urls.append(seeds["zillow"]["zip_search"].format(ZIP=z))
    for city, state in cities_states:
        urls.append(seeds["zillow"]["city_search"].format(CITY=_slug_city(city), STATE=state))
    return urls

def build_redfin_urls(zips, cities_states, seeds, city_ids: dict | None = None):
    urls = []
    for z in zips:
        urls.append(seeds["redfin"]["zip_search"].format(ZIP=z))
    for city, state in cities_states:
        cid = None
        if city_ids:
            cid = city_ids.get(f"{city},{state}") or city_ids.get((city, state))
        if cid is None:
            # لو ما عندك CITY_ID تجاهل هذا الرابط
            continue
        urls.append(seeds["redfin"]["city_search"].format(CITY_ID=cid, CITY=_slug_city(city), STATE=state))
    return urls

def balanced_mix(zips, cities_states, seeds, city_ids, total=10, per_platform_min=5):
    z = build_zillow_urls(zips, cities_states, seeds)
    r = build_redfin_urls(zips, cities_states, seeds, city_ids)
    random.shuffle(z); random.shuffle(r)

    take_z = min(max(per_platform_min, total // 2), len(z))
    take_r = min(total - take_z, len(r))
    mixed = []
    for i in range(max(take_z, take_r)):
        if i < take_z: mixed.append(("zillow", z[i]))
        if len(mixed) >= total: break
        if i < take_r: mixed.append(("redfin", r[i]))
        if len(mixed) >= total: break
    return mixed[:total]
