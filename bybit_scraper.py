import asyncio
import websockets
import json
import time
import csv
import os
import argparse
from collections import deque
from typing import Dict, List, Tuple, Optional
import statistics

class BybitOrderbookStream:
    def __init__(self, symbol: str = "BTCUSDT", save_data: bool = True, vwap_depth: float = 10000):
        self.symbol = symbol
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        self.orderbook = {"bids": [], "asks": []}
        self.last_update = None
        self.price_history = deque(maxlen=100)  # Store last 100 mid prices
        self.vwap_history = deque(maxlen=50)   # Store last 50 VWAP calculations
        self.vwap_depth = vwap_depth  # Make VWAP depth configurable
        
        # Volume tracking
        self.total_bid_volume = deque(maxlen=100)  # Track bid volume over time
        self.total_ask_volume = deque(maxlen=100)  # Track ask volume over time
        self.volume_history = deque(maxlen=100)    # Track total volume
        
        # Data logging setup
        self.save_data = save_data
        self.data_log = []
        if save_data:
            self.csv_filename = f"{symbol}_orderbook_data_{int(time.time())}.csv"
            self.csv_headers = [
                "timestamp", "datetime", "mid_price", "vwap_10k", "microprice", 
                "price_ema", "spread_abs", "spread_bps", "bid_liquidity_top5", 
                "ask_liquidity_top5", "imbalance", "best_bid", "best_ask",
                "total_bid_volume", "total_ask_volume", "total_volume", "bid_volume_top10", "ask_volume_top10"
            ]
        
    async def connect(self):
        """Connect to Bybit WebSocket and subscribe to orderbook updates"""
        try:
            async with websockets.connect(self.ws_url) as websocket:
                # Subscribe to orderbook depth
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [f"orderbook.50.{self.symbol}"]
                }
                await websocket.send(json.dumps(subscribe_msg))
                print(f"🔗 Connected to Bybit WebSocket for {self.symbol}")
                
                async for message in websocket:
                    await self.handle_message(message)
                    
        except Exception as e:
            print(f"❌ WebSocket connection error: {e}")
            # Reconnect after 5 seconds
            await asyncio.sleep(5)
            await self.connect()
    
    async def handle_message(self, message: str):
        """Process incoming WebSocket messages"""
        try:
            data = json.loads(message)
            
            if data.get("topic") == f"orderbook.50.{self.symbol}":
                self.update_orderbook(data["data"])
                await self.calculate_fair_prices()
                
        except Exception as e:
            print(f"❌ Error handling message: {e}")
    
    def update_orderbook(self, data: Dict):
        """Update local orderbook with new data"""
        if "b" in data:  # bids
            self.orderbook["bids"] = [[float(price), float(size)] for price, size in data["b"]]
        if "a" in data:  # asks
            self.orderbook["asks"] = [[float(price), float(size)] for price, size in data["a"]]
        
        self.last_update = time.time()
    
    def calculate_volume_metrics(self) -> Dict:
        """Calculate volume metrics from orderbook"""
        if not self.orderbook["bids"] or not self.orderbook["asks"]:
            return {}
        
        # Total volume in orderbook
        total_bid_volume = sum(size for price, size in self.orderbook["bids"])
        total_ask_volume = sum(size for price, size in self.orderbook["asks"])
        total_volume = total_bid_volume + total_ask_volume
        
        # Volume in top 10 levels
        bid_volume_top10 = sum(size for price, size in self.orderbook["bids"][:10])
        ask_volume_top10 = sum(size for price, size in self.orderbook["asks"][:10])
        
        # Store volume history
        self.total_bid_volume.append(total_bid_volume)
        self.total_ask_volume.append(total_ask_volume)
        self.volume_history.append(total_volume)
        
        return {
            "total_bid_volume": total_bid_volume,
            "total_ask_volume": total_ask_volume,
            "total_volume": total_volume,
            "bid_volume_top10": bid_volume_top10,
            "ask_volume_top10": ask_volume_top10,
            "volume_imbalance": (total_bid_volume - total_ask_volume) / total_volume if total_volume > 0 else 0
        }
    
    def calculate_mid_price(self) -> Optional[float]:
        """Calculate simple mid price"""
        if not self.orderbook["bids"] or not self.orderbook["asks"]:
            return None
            
        best_bid = self.orderbook["bids"][0][0]
        best_ask = self.orderbook["asks"][0][0]
        
        return (best_bid + best_ask) / 2
    
    def calculate_vwap(self, depth_usd: float = None) -> Optional[float]:
        """Calculate Volume Weighted Average Price for given depth"""
        if depth_usd is None:
            depth_usd = self.vwap_depth
            
        if not self.orderbook["bids"] or not self.orderbook["asks"]:
            return None
        
        # Calculate VWAP for both sides
        bid_vwap = self._calculate_side_vwap(self.orderbook["bids"], depth_usd, "bid")
        ask_vwap = self._calculate_side_vwap(self.orderbook["asks"], depth_usd, "ask")
        
        if bid_vwap is None or ask_vwap is None:
            return None
            
        return (bid_vwap + ask_vwap) / 2
    
    def _calculate_side_vwap(self, orders: List[List[float]], depth_usd: float, side: str) -> Optional[float]:
        """Calculate VWAP for one side of the book"""
        total_volume = 0
        total_value = 0
        
        for price, size in orders:
            order_value = price * size
            
            if total_value + order_value <= depth_usd:
                # Full order fits within depth
                total_volume += size
                total_value += order_value
            else:
                # Partial order to reach exact depth
                remaining_value = depth_usd - total_value
                remaining_size = remaining_value / price
                total_volume += remaining_size
                total_value += remaining_value
                break
        
        if total_volume == 0:
            return None
            
        return total_value / total_volume
    
    def calculate_weighted_mid(self, bid_weight: float = 0.5) -> Optional[float]:
        """Calculate weighted mid price (useful for adjusting for inventory)"""
        if not self.orderbook["bids"] or not self.orderbook["asks"]:
            return None
            
        best_bid = self.orderbook["bids"][0][0]
        best_ask = self.orderbook["asks"][0][0]
        
        return (best_bid * bid_weight) + (best_ask * (1 - bid_weight))
    
    def calculate_microprice(self) -> Optional[float]:
        """Calculate microprice using bid/ask sizes as weights"""
        if not self.orderbook["bids"] or not self.orderbook["asks"]:
            return None
            
        best_bid_price = self.orderbook["bids"][0][0]
        best_bid_size = self.orderbook["bids"][0][1]
        best_ask_price = self.orderbook["asks"][0][0]
        best_ask_size = self.orderbook["asks"][0][1]
        
        total_size = best_bid_size + best_ask_size
        
        if total_size == 0:
            return None
            
        return (best_bid_price * best_ask_size + best_ask_price * best_bid_size) / total_size
    
    def get_spread_metrics(self) -> Dict:
        """Get spread and liquidity metrics"""
        if not self.orderbook["bids"] or not self.orderbook["asks"]:
            return {}
            
        best_bid = self.orderbook["bids"][0][0]
        best_ask = self.orderbook["asks"][0][0]
        
        spread_abs = best_ask - best_bid
        spread_bps = (spread_abs / ((best_bid + best_ask) / 2)) * 10000
        
        # Calculate liquidity at different levels
        bid_liquidity_5 = sum(size for price, size in self.orderbook["bids"][:5])
        ask_liquidity_5 = sum(size for price, size in self.orderbook["asks"][:5])
        
        return {
            "spread_abs": spread_abs,
            "spread_bps": spread_bps,
            "bid_liquidity_top5": bid_liquidity_5,
            "ask_liquidity_top5": ask_liquidity_5,
            "imbalance": (bid_liquidity_5 - ask_liquidity_5) / (bid_liquidity_5 + ask_liquidity_5)
        }
    
    async def calculate_fair_prices(self):
        """Calculate and display all fair price metrics"""
        mid_price = self.calculate_mid_price()
        vwap = self.calculate_vwap()
        microprice = self.calculate_microprice()
        spread_metrics = self.get_spread_metrics()
        volume_metrics = self.calculate_volume_metrics()
        
        if mid_price:
            self.price_history.append(mid_price)
        
        if vwap:
            self.vwap_history.append(vwap)
        
        # Calculate moving averages
        price_ema = self.calculate_ema(list(self.price_history), 0.1) if len(self.price_history) > 1 else mid_price
        
        # Print summary (reduced frequency for 1-minute runs)
        if len(self.price_history) % 10 == 0:  # Print every 10th update
            timestamp_str = time.strftime("%H:%M:%S", time.localtime())
            print(f"\n📊 {self.symbol} Fair Price Summary (Update #{len(self.price_history)}) [{timestamp_str}]:")
            print(f"   Mid Price: ${mid_price:.4f}" if mid_price is not None else "   Mid Price: N/A")
            print(f"   VWAP ({self.vwap_depth/1000:.0f}k): ${vwap:.4f}" if vwap is not None else f"   VWAP: N/A")
            print(f"   Microprice: ${microprice:.4f}" if microprice is not None else "   Microprice: N/A")
            print(f"   EMA Price: ${price_ema:.4f}" if price_ema is not None else "   EMA: N/A")
            print(f"   Spread: {spread_metrics.get('spread_bps', 0):.2f} bps" if spread_metrics.get('spread_bps') is not None else "   Spread: N/A")
            print(f"   Imbalance: {spread_metrics.get('imbalance', 0):.3f}" if spread_metrics.get('imbalance') is not None else "   Imbalance: N/A")
            print(f"   Total Volume: {volume_metrics.get('total_volume', 0):,.1f}" if volume_metrics.get('total_volume') is not None else "   Volume: N/A")
            print(f"   Volume Imbalance: {volume_metrics.get('volume_imbalance', 0):.3f}" if volume_metrics.get('volume_imbalance') is not None else "   Vol Imbalance: N/A")
        
        # Return data for external use
        result = {
            "timestamp": time.time(),
            "mid_price": mid_price,
            "vwap_10k": vwap,
            "microprice": microprice,
            "price_ema": price_ema,
            "spread_metrics": spread_metrics,
            "volume_metrics": volume_metrics
        }
        
        # Save data to log and CSV
        if self.save_data:
            self.save_to_csv(result)
        
        return result
    
    def calculate_ema(self, prices: List[float], alpha: float) -> float:
        """Calculate Exponential Moving Average"""
        if not prices:
            return None
            
        ema = prices[0]
        for price in prices[1:]:
            ema = alpha * price + (1 - alpha) * ema
        return ema
    
    def get_orderbook_snapshot(self, levels: int = 10) -> Dict:
        """Get current orderbook snapshot"""
        return {
            "symbol": self.symbol,
            "timestamp": self.last_update,
            "bids": self.orderbook["bids"][:levels],
            "asks": self.orderbook["asks"][:levels]
        }
    
    def save_to_csv(self, data: Dict):
        """Save data point to CSV file"""
        try:
            timestamp = data["timestamp"]
            datetime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            spread_metrics = data["spread_metrics"]
            volume_metrics = data["volume_metrics"]
            
            # Prepare row data
            row = [
                timestamp,
                datetime_str,
                data["mid_price"],
                data["vwap_10k"],
                data["microprice"],
                data["price_ema"],
                spread_metrics.get("spread_abs"),
                spread_metrics.get("spread_bps"),
                spread_metrics.get("bid_liquidity_top5"),
                spread_metrics.get("ask_liquidity_top5"),
                spread_metrics.get("imbalance"),
                self.orderbook["bids"][0][0] if self.orderbook["bids"] else None,
                self.orderbook["asks"][0][0] if self.orderbook["asks"] else None,
                volume_metrics.get("total_bid_volume"),
                volume_metrics.get("total_ask_volume"),
                volume_metrics.get("total_volume"),
                volume_metrics.get("bid_volume_top10"),
                volume_metrics.get("ask_volume_top10")
            ]
            
            # Write to CSV
            file_exists = os.path.isfile(self.csv_filename)
            with open(self.csv_filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(self.csv_headers)
                writer.writerow(row)
                
        except Exception as e:
            print(f"❌ Error saving to CSV: {e}")
    
    def get_session_summary(self) -> Dict:
        """Get summary statistics for the session"""
        if not self.price_history:
            return {}
        
        # Volume statistics
        volume_stats = {}
        if self.volume_history:
            volume_stats = {
                "avg_total_volume": sum(self.volume_history) / len(self.volume_history),
                "min_total_volume": min(self.volume_history),
                "max_total_volume": max(self.volume_history),
                "final_total_volume": self.volume_history[-1],
                "avg_bid_volume": sum(self.total_bid_volume) / len(self.total_bid_volume) if self.total_bid_volume else 0,
                "avg_ask_volume": sum(self.total_ask_volume) / len(self.total_ask_volume) if self.total_ask_volume else 0
            }
            
        return {
            "total_updates": len(self.price_history),
            "price_min": min(self.price_history),
            "price_max": max(self.price_history),
            "price_avg": sum(self.price_history) / len(self.price_history),
            "final_price": self.price_history[-1],
            "csv_file": self.csv_filename if self.save_data else None,
            **volume_stats
        }

# Example usage functions (keeping original functionality)
async def main():
    """Original main function"""
    bot = BybitOrderbookStream("BTCUSDT")
    
    print("🚀 Starting Crypto MM Bot - Orderbook Stream")
    print("   Running for 60 seconds...")
    
    try:
        await asyncio.wait_for(bot.connect(), timeout=60.0)
    except asyncio.TimeoutError:
        print("\n⏰ 60 seconds completed - stopping bot...")
    except KeyboardInterrupt:
        print("\n🛑 Stopping bot...")

async def main_with_custom_duration(duration_seconds: int = 60):
    """Original custom duration function"""
    bot = BybitOrderbookStream("DOTUSDC", save_data=True)
    
    print(f"🚀 Starting Crypto MM Bot - Running for {duration_seconds} seconds")
    print(f"📁 Data will be saved to: {bot.csv_filename}")
    start_time = time.time()
    
    try:
        await asyncio.wait_for(bot.connect(), timeout=duration_seconds)
    except asyncio.TimeoutError:
        elapsed = time.time() - start_time
        print(f"\n⏰ {elapsed:.1f} seconds completed - stopping bot...")
        
        summary = bot.get_session_summary()
        if summary:
            print(f"📊 Session Summary:")
            print(f"   Total price updates: {summary['total_updates']}")
            print(f"   Price range: ${summary['price_min']:.4f} - ${summary['price_max']:.4f}")
            print(f"   Average price: ${summary['price_avg']:.4f}")
            print(f"   Final price: ${summary['final_price']:.4f}")
            print(f"   Data saved to: {summary['csv_file']}")
    except KeyboardInterrupt:
        print("\n🛑 Stopping bot...")
        summary = bot.get_session_summary()
        if summary and summary.get('csv_file'):
            print(f"📁 Data saved to: {summary['csv_file']}")

# NEW: Configurable main function
async def main_configurable(symbol: str = "BTCUSDT", duration: int = 60, save_csv: bool = True, vwap_depth: float = 10000):
    """Run bot with configurable parameters"""
    print(f"🚀 Starting Crypto MM Bot")
    print(f"   Symbol: {symbol}")
    print(f"   Duration: {duration} seconds")
    print(f"   Save CSV: {'Yes' if save_csv else 'No'}")
    print(f"   VWAP Depth: ${vwap_depth:,.0f}")
    print("-" * 50)
    
    bot = BybitOrderbookStream(symbol, save_data=save_csv, vwap_depth=vwap_depth)
    
    if save_csv:
        print(f"📁 Data will be saved to: {bot.csv_filename}")
    
    start_time = time.time()
    
    try:
        await asyncio.wait_for(bot.connect(), timeout=duration)
    except asyncio.TimeoutError:
        elapsed = time.time() - start_time
        print(f"\n⏰ {elapsed:.1f} seconds completed - stopping bot...")
        
        summary = bot.get_session_summary()
        if summary:
            print(f"📊 Session Summary:")
            print(f"   Total price updates: {summary['total_updates']}")
            print(f"   Price range: ${summary['price_min']:.4f} - ${summary['price_max']:.4f}")
            print(f"   Average price: ${summary['price_avg']:.4f}")
            print(f"   Final price: ${summary['final_price']:.4f}")
            # Volume metrics
            if 'avg_total_volume' in summary:
                print(f"   Average Total Volume: {summary['avg_total_volume']:,.1f}")
                print(f"   Volume Range: {summary['min_total_volume']:,.1f} - {summary['max_total_volume']:,.1f}")
                print(f"   Final Volume: {summary['final_total_volume']:,.1f}")
                print(f"   Avg Bid/Ask Volume: {summary['avg_bid_volume']:,.1f} / {summary['avg_ask_volume']:,.1f}")
            if save_csv:
                print(f"   Data saved to: {summary['csv_file']}")
    except KeyboardInterrupt:
        print("\n🛑 Stopping bot...")
        summary = bot.get_session_summary()
        if summary and summary.get('csv_file') and save_csv:
            print(f"📁 Data saved to: {summary['csv_file']}")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Bybit Orderbook Stream Bot')
    
    parser.add_argument('--symbol', '-s', type=str, default='BTCUSDT',
                       help='Trading pair symbol (default: BTCUSDT)')
    
    parser.add_argument('--duration', '-d', type=int, default=60,
                       help='Duration to run in seconds (default: 60)')
    
    parser.add_argument('--no-csv', action='store_true',
                       help='Disable CSV logging (default: enabled)')
    
    parser.add_argument('--vwap-depth', type=float, default=10000,
                       help='VWAP calculation depth in USD (default: 10000)')
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Run with parsed arguments
    asyncio.run(main_configurable(
        symbol=args.symbol,
        duration=args.duration,
        save_csv=not args.no_csv,
        vwap_depth=args.vwap_depth
    ))