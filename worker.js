addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  const url = new URL(request.url)
  const targetUrl = url.searchParams.get("d")
  
  if (!targetUrl) {
    return new Response("Missing download URL. Use ?d=<url>", { status: 400 })
  }
  
  try {
    const targetUrlObj = new URL(targetUrl)
    
    // ब्राउज़र जैसे हेडर्स जो Cloudflare को खुश करेंगे
    const browserHeaders = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'gzip, deflate, br',
      'DNT': '1',
      'Connection': 'keep-alive',
      'Upgrade-Insecure-Requests': '1',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'none',
      'Sec-Fetch-User': '?1',
      'Cache-Control': 'max-age=0',
      'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
      'Referer': 'https://kwik.cx/',
    }
    
    // पहला प्रयास
    let response = await fetch(targetUrl, { headers: browserHeaders })
    
    // अगर 403 आए तो वैकल्पिक रेफरर के साथ पुनः प्रयास करें
    if (response.status === 403) {
      const altHeaders = { ...browserHeaders, 'Referer': targetUrlObj.origin + '/' }
      response = await fetch(targetUrl, { headers: altHeaders })
      
      // अगर फिर भी 403 आए तो तीसरा प्रयास
      if (response.status === 403) {
        const thirdHeaders = { ...browserHeaders, 'Referer': 'https://animepahe.com/' }
        response = await fetch(targetUrl, { headers: thirdHeaders })
      }
    }
    
    // अगर सभी प्रयास विफल रहें
    if (response.status === 403) {
      return new Response("Unable to bypass Cloudflare protection. The site may require additional authentication.", { status: 403 })
    }
    
    // सफल रिस्पांस को वापस भेजें
    return createResponse(response, targetUrl)
    
  } catch (error) {
    return new Response(`Error: ${error.message}`, { status: 500 })
  }
}

function createResponse(response, targetUrl) {
  const contentType = response.headers.get("Content-Type") || "application/octet-stream"
  let contentDisposition = response.headers.get("Content-Disposition")
  
  if (!contentDisposition) {
    const urlObj = new URL(targetUrl)
    const pathname = urlObj.pathname
    const filename = pathname.substring(pathname.lastIndexOf('/') + 1) || "video.mp4"
    contentDisposition = `attachment; filename="${filename}"`
  }
  
  return new Response(response.body, {
    status: response.status,
    headers: {
      'Content-Type': contentType,
      'Content-Disposition': contentDisposition,
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
      'Access-Control-Allow-Headers': 'Origin, X-Requested-With, Content-Type, Accept, Authorization'
    }
  })
}
