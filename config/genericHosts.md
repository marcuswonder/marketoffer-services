Generic Host Filtering

Policy
- Block common search engines, social networks, review sites, business directories, property portals, marketplaces, and public-data sites.
- Use base domains only; the app filters both exact matches and any subdomains (host endsWith .domain).
- Prefer deterministic rules; keep list focused and review periodically.

How It’s Used
- companyDiscovery loads `genericHosts.json` and excludes any result whose host equals or ends with a listed domain.
- The list is lowercased at load time.

Maintenance Tips
- Add base domains (e.g., `yelp.com`), not subdomains (e.g., `m.yelp.com`).
- Avoid adding specific brand/company websites unless they are truly directory-like; overblocking increases false negatives.
- Consider adding platform bases instead of many regional subdomains (e.g., `pinterest.com`, `wordpress.com`).
- If a domain should be allowed despite matching this list, add it to an allowlist in code or a separate file (future enhancement).

Categories (examples)
- Search/Maps: google.com, bing.com, duckduckgo.com, yahoo.com, mapquest.com
- Social/Media: facebook.com, instagram.com, twitter.com, x.com, youtube.com, vimeo.com, reddit.com, pinterest.com, tumblr.com
- Reviews/Directories: trustpilot.com, reviews.io, yelp.com, yotpo.com, freeindex.co.uk, thomsonlocal.com, yell.com
- Property: rightmove.co.uk, zoopla.co.uk, onthemarket.com, primelocation.com, placebuzz.com, nestoria.co.uk, home.co.uk
- Company Data: find-and-update.company-information.service.gov.uk, companieshouse.gov.uk, opencorporates.com, endole.co.uk, companycheck.co.uk
- Prospect/Data Vendors: crunchbase.com, zoominfo.com, rocketreach.co, dnb.com, kompass.com, datanyze.com
- Hosted/Blogs: wordpress.com, wixsite.com, squarespace.com, weebly.com, medium.com, blogspot.com, github.io, notion.site, webflow.io, netlify.app

