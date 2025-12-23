## 🚀 Key Features

### ⚡ Parallel Processing Engine
- Multi-threaded architecture using `ThreadPoolExecutor`
- Runs multiple browser instances simultaneously
- Configurable worker threads for scalable scraping
- Optimized task scheduling to keep all threads busy
- Significantly faster than sequential scraping

---

### 📊 Comprehensive Product Data Extraction
- Product title and brand  
- Selling price and regular price  
- Unit price (price per count / unit)  
- Product availability (In Stock / Out of Stock / Not Listed)  
- Seller information (Walmart / Marketplace sellers)  
- Customer ratings and review counts  
- High-resolution image URLs and image count  
- Product description and canonical URL  
- GTIN / Model identifiers (where available)  

---

### 🗺️ Multi-ZIP Code Support
- Scrape the same product across multiple ZIP codes
- Compare availability and pricing by location
- Each ZIP code runs in an isolated browser profile
- Easy configuration for adding or removing ZIP codes

---

### 🧠 Intelligent Scraping Design
- Uses **Undetected ChromeDriver** to reduce bot detection
- Persistent Chrome profiles for improved session stability
- Automatic retry logic for transient failures
- Thread-safe CSV writing to avoid data corruption
- Resume capability using container CSVs if execution is interrupted

--

### 📁 Structured Output
- Intermediate data stored as CSV (thread-safe)
- Final consolidated Excel output (`.xlsx`)
- Separate sheets for:
  - Product details
  - Sellers
  - Reviews
  - Variations (if enabled)
- Cleaned and deduplicated records

## ⚠️ Important Notes

### Legal & Ethical Usage
- For educational and internal analytics purposes only
- Respect Walmart’s terms of service
