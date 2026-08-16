"""
Example 4: Google Gemini 2.0 / 3.0 Tool Calling & Orderflow Briefing Integration.
"""

import asyncio
from crypto_liquidation.gemini import fetch_live_liquidations, get_liquidation_tools, get_gemini_analysis_prompt

async def main():
    print("🤖 [Gemini Function Calling Simulation]")
    print("Tool definitions ready for Google GenAI SDK (client.models.generate_content):")
    tools = get_liquidation_tools()
    print(f"• Tool Name: {tools[0]['name']}")
    print(f"• Parameters: {list(tools[0]['parameters']['properties'].keys())}\n")

    print("⚡ Gemini is invoking `fetch_live_liquidations(symbols=['BTCUSDT', 'ETHUSDT'], duration_sec=5)`...")
    
    # 5-second live sampling
    report = await fetch_live_liquidations(symbols=["BTCUSDT", "ETHUSDT"], duration_sec=5)
    
    print("\n--- Output returned to Gemini's Context Window ---")
    print(report)
    print("-------------------------------------------------")
    
    print("\n📈 [Gemini Orderflow Analysis Directives]")
    print(get_gemini_analysis_prompt())

if __name__ == "__main__":
    asyncio.run(main())
