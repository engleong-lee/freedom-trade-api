"""
Alpaca Trading API v3 - Revamped Implementation
Based on research recommendations for proper bracket/OTO orders and robust error handling
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest, LimitOrderRequest, 
    ReplaceOrderRequest, TakeProfitRequest, 
    StopLossRequest, StopOrderRequest,
    GetOrdersRequest
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass, QueryOrderStatus, OrderStatus
from alpaca.data.historical import StockHistoricalDataClient
import json
from typing import Dict, Optional, List
from datetime import datetime
import logging
import asyncio
import time
import os
import inspect
from fastapi.middleware.cors import CORSMiddleware

# ===========================
# CONFIGURATION
# ===========================

API_KEY = os.getenv("ALPACA_API_KEY", "PKJBU8P5YSMVU3QX44BX")
API_SECRET = os.getenv("ALPACA_API_SECRET", "qFKl0DytBaTAEcqIdzyUlTcMZFNgWqehjngpzIFy")
ENABLE_EXTENDED_HOURS = os.getenv("ENABLE_EXTENDED_HOURS", "false").lower() == "true"

# Initialize clients
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)
stock_client = StockHistoricalDataClient(API_KEY, API_SECRET)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Alpaca Trading API v3 - Revamped")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===========================
# POSITION TRACKER
# ===========================

class PositionTracker:
    """Enhanced position tracking with persistent storage"""
    
    def __init__(self):
        self.positions = {}  # {symbol: position_data}
        
    def track_position(self, symbol: str, data: Dict):
        """Track a new or updated position"""
        self.positions[symbol] = {
            **data,
            'last_updated': datetime.now().isoformat()
        }
        
    def get_position(self, symbol: str) -> Optional[Dict]:
        """Get tracked position data"""
        return self.positions.get(symbol)
        
    def remove_position(self, symbol: str):
        """Remove position from tracking"""
        if symbol in self.positions:
            del self.positions[symbol]
            
    def get_all_positions(self) -> Dict:
        """Get all tracked positions"""
        return self.positions.copy()

position_tracker = PositionTracker()

# ===========================
# CORE TRADING EXECUTOR
# ===========================

class AlpacaTradingExecutor:
    """Main trading executor with enhanced error handling"""
    
    def __init__(self):
        self.client = trading_client
        self.logger = logger
        self.position_tracker = position_tracker
        self.max_retries = 3
        self.retry_delay = 2
        
    async def execute_with_retry(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry"""
        import inspect
        
        for attempt in range(self.max_retries):
            try:
                # Check if the function is async
                if inspect.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    # For synchronous functions, just call them directly
                    return func(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise e
                    
                wait_time = self.retry_delay ** (attempt + 1)
                self.logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
        
    # ===========================
    # SCENARIO 1: NEW TRADE
    # ===========================
    
    async def handle_new_trade(self, decision: Dict) -> Dict:
        """Handle both swing and trend new trade entries with proper order classes"""
        trade_info = decision.get('new_trade', {})
        symbol = decision.get('symbol')
        strategy = trade_info.get('strategy')
        
        self.logger.info(f"Creating {strategy} trade for {symbol}")
        
        if strategy == 'SWING':
            return await self._place_swing_trade(symbol, trade_info)
        elif strategy == 'TREND':
            return await self._place_trend_trade(symbol, trade_info)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")
    
    async def _place_swing_trade(self, symbol: str, trade_info: Dict) -> Dict:
        """Place a bracket order for swing trading"""
        try:
            # Prepare bracket order with stop-loss and take-profit
            order_data = LimitOrderRequest(
                symbol=symbol,
                qty=trade_info['qty'],
                side=OrderSide.BUY if trade_info['side'].lower() == 'buy' else OrderSide.SELL,
                time_in_force=TimeInForce.GTC,
                limit_price=trade_info['limit_price'],
                order_class=OrderClass.BRACKET,
                # Stop-loss: using stop-market for guaranteed execution
                stop_loss=StopLossRequest(
                    stop_price=trade_info['stop_loss']['stop_price']
                ),
                # Take-profit: limit order at target
                take_profit=TakeProfitRequest(
                    limit_price=trade_info['take_profit']['limit_price']
                ),
                extended_hours=ENABLE_EXTENDED_HOURS
            )
            
            # Submit the bracket order
            order = await self.execute_with_retry(
                self.client.submit_order,
                order_data=order_data
            )
            
            # Store order IDs for tracking
            tracking_data = {
                'main_order_id': str(order.id),
                'strategy': 'SWING',
                'entry_price': trade_info['limit_price'],
                'stop_loss': trade_info['stop_loss']['stop_price'],
                'take_profit': trade_info['take_profit']['limit_price'],
                'qty': trade_info['qty'],
                'pattern': trade_info.get('pattern'),
                'timestamp': datetime.now().isoformat()
            }
            
            # Get the child orders (stop-loss and take-profit)
            await asyncio.sleep(1)  # Brief delay for child orders to be created
            
            all_orders = self.client.get_orders(
                filter=GetOrdersRequest(
                    status=QueryOrderStatus.OPEN,
                    symbols=[symbol]
                )
            )
            
            child_orders = [o for o in all_orders if hasattr(o, 'parent_id') and o.parent_id == order.id]
            
            for child in child_orders:
                if child.order_type == 'stop':
                    tracking_data['stop_order_id'] = str(child.id)
                elif child.order_type == 'limit' and child.side != order.side:
                    tracking_data['profit_order_id'] = str(child.id)
            
            self.position_tracker.track_position(symbol, tracking_data)
            
            return {
                'success': True,
                'action': 'NEW_TRADE',
                'order_id': str(order.id),
                'message': f"Swing trade placed: {symbol} @ {trade_info['limit_price']}",
                'details': tracking_data
            }
            
        except Exception as e:
            self.logger.error(f"Failed to place swing trade: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _place_trend_trade(self, symbol: str, trade_info: Dict) -> Dict:
        """Place an OTO order for trend trading (entry with stop-loss only)"""
        try:
            # For trend trades, we use OTO order class (no take-profit)
            order_data = LimitOrderRequest(
                symbol=symbol,
                qty=trade_info['qty'],
                side=OrderSide.BUY if trade_info['side'].lower() == 'buy' else OrderSide.SELL,
                time_in_force=TimeInForce.GTC,
                limit_price=trade_info['limit_price'],
                order_class=OrderClass.OTO,  # One-Triggers-Other
                stop_loss=StopLossRequest(
                    stop_price=trade_info['stop_loss']['stop_price']
                ),
                extended_hours=ENABLE_EXTENDED_HOURS
            )
            
            order = await self.execute_with_retry(
                self.client.submit_order,
                order_data=order_data
            )
            
            # Track the position
            tracking_data = {
                'main_order_id': str(order.id),
                'strategy': 'TREND',
                'entry_price': trade_info['limit_price'],
                'stop_loss': trade_info['stop_loss']['stop_price'],
                'qty': trade_info['qty'],
                'pattern': trade_info.get('pattern'),
                'timestamp': datetime.now().isoformat(),
                'trailing_enabled': False
            }
            
            # Find and store stop-loss order ID
            await asyncio.sleep(1)
            all_orders = self.client.get_orders(
                filter=GetOrdersRequest(
                    status=QueryOrderStatus.OPEN,
                    symbols=[symbol]
                )
            )
            
            for o in all_orders:
                if hasattr(o, 'parent_id') and o.parent_id == order.id and o.order_type == 'stop':
                    tracking_data['stop_order_id'] = str(o.id)
                    break
            
            self.position_tracker.track_position(symbol, tracking_data)
            
            return {
                'success': True,
                'action': 'NEW_TRADE',
                'order_id': str(order.id),
                'message': f"Trend trade placed: {symbol} @ {trade_info['limit_price']}",
                'details': tracking_data
            }
            
        except Exception as e:
            self.logger.error(f"Failed to place trend trade: {e}")
            return {'success': False, 'error': str(e)}
    
    # ===========================
    # SCENARIO 2: ADD POSITION
    # ===========================
    
    async def handle_add_position(self, decision: Dict) -> Dict:
        """Add to existing position with combined stop-loss management"""
        add_info = decision.get('add_position', {})
        symbol = decision.get('symbol')
        new_position = add_info.get('new_position', {})
        
        try:
            # Get current position details
            current_position = self.client.get_open_position(symbol)
            current_qty = int(current_position.qty)
            
            # Get tracking data
            tracked = self.position_tracker.get_position(symbol)
            
            # Cancel existing stop-loss order if it exists
            if tracked and 'stop_order_id' in tracked:
                try:
                    await self.execute_with_retry(
                        self.client.cancel_order_by_id,
                        tracked['stop_order_id']
                    )
                    self.logger.info(f"Cancelled existing stop-loss for {symbol}")
                except Exception as e:
                    self.logger.warning(f"Could not cancel stop order: {e}")
            
            # Wait for cancellation to process
            await asyncio.sleep(1)
            
            # Place the new limit order to add to position
            add_order_data = LimitOrderRequest(
                symbol=symbol,
                qty=new_position['qty'],
                side=OrderSide.BUY if new_position['side'].lower() == 'buy' else OrderSide.SELL,
                time_in_force=TimeInForce.GTC,
                limit_price=new_position['limit_price'],
                order_class=OrderClass.SIMPLE,
                extended_hours=ENABLE_EXTENDED_HOURS
            )
            
            add_order = await self.execute_with_retry(
                self.client.submit_order,
                order_data=add_order_data
            )
            
            # Monitor for fill (max 30 seconds)
            filled_qty = await self._monitor_order_fill(str(add_order.id), max_wait=30)
            
            # Calculate combined position
            combined_qty = current_qty + filled_qty
            combined_stop_price = add_info['updated_stop_loss_all_positions']
            
            if combined_qty > current_qty:
                # Create new stop-loss for combined position
                stop_side = OrderSide.SELL if new_position['side'].lower() == 'buy' else OrderSide.BUY
                
                combined_stop_data = StopOrderRequest(
                    symbol=symbol,
                    qty=combined_qty,
                    side=stop_side,
                    time_in_force=TimeInForce.GTC,
                    stop_price=combined_stop_price,
                    extended_hours=ENABLE_EXTENDED_HOURS
                )
                
                stop_order = await self.execute_with_retry(
                    self.client.submit_order,
                    order_data=combined_stop_data
                )
                
                # Update tracking
                if tracked:
                    tracked.update({
                        'add_order_id': str(add_order.id),
                        'stop_order_id': str(stop_order.id),
                        'combined_qty': combined_qty,
                        'stop_loss': combined_stop_price,
                        'last_add_timestamp': datetime.now().isoformat()
                    })
                    self.position_tracker.track_position(symbol, tracked)
                
                return {
                    'success': True,
                    'action': 'ADD_POSITION',
                    'add_order_id': str(add_order.id),
                    'stop_order_id': str(stop_order.id),
                    'message': f"Added {filled_qty} shares to {symbol} position",
                    'combined_position': {
                        'total_qty': combined_qty,
                        'stop_loss': combined_stop_price
                    }
                }
            else:
                return {
                    'success': False,
                    'action': 'ADD_POSITION',
                    'message': "Add order not filled",
                    'add_order_id': str(add_order.id)
                }
                
        except Exception as e:
            self.logger.error(f"Failed to add position: {e}")
            return {'success': False, 'error': str(e)}
    
    # ===========================
    # SCENARIO 3: ADJUST STOP LOSS
    # ===========================
    
    async def handle_adjust_stop_loss(self, decision: Dict) -> Dict:
        """Adjust stop-loss for existing position"""
        symbol = decision.get('symbol')
        adjust_info = decision.get('adjust_stop_loss', {})
        new_stop_price = adjust_info['new_stop_loss']
        
        try:
            # Get current position
            position = self.client.get_open_position(symbol)
            qty = int(position.qty)
            
            # Determine stop side (opposite of position)
            stop_side = OrderSide.SELL if float(position.qty) > 0 else OrderSide.BUY
            
            # Get tracking data
            tracked = self.position_tracker.get_position(symbol)
            existing_stop_id = tracked.get('stop_order_id') if tracked else None
            
            # If no tracked stop, search for it
            if not existing_stop_id:
                all_orders = self.client.get_orders(
                    filter=GetOrdersRequest(
                        status=QueryOrderStatus.OPEN,
                        symbols=[symbol]
                    )
                )
                
                for order in all_orders:
                    if order.order_type in ['stop', 'stop_limit']:
                        existing_stop_id = str(order.id)
                        break
            
            if existing_stop_id:
                # Try to replace the order
                try:
                    replace_request = ReplaceOrderRequest(
                        stop_price=new_stop_price,
                        qty=qty
                    )
                    
                    updated_order = await self.execute_with_retry(
                        self.client.replace_order_by_id,
                        order_id=existing_stop_id,
                        order_data=replace_request
                    )
                    
                    # Update tracking
                    if tracked:
                        tracked['stop_loss'] = new_stop_price
                        tracked['stop_adjusted_timestamp'] = datetime.now().isoformat()
                        self.position_tracker.track_position(symbol, tracked)
                    
                    return {
                        'success': True,
                        'action': 'ADJUST_STOP_LOSS',
                        'order_id': str(updated_order.id),
                        'message': f"Stop-loss adjusted to {new_stop_price}",
                        'reason': adjust_info.get('reason', 'Manual adjustment')
                    }
                    
                except Exception as replace_error:
                    # If replace fails, cancel and create new
                    self.logger.warning(f"Replace failed, using cancel/recreate: {replace_error}")
                    return await self._recreate_stop_loss(
                        symbol, qty, stop_side, new_stop_price, adjust_info
                    )
            else:
                # No existing stop found, create new one
                return await self._create_new_stop_loss(
                    symbol, qty, stop_side, new_stop_price, adjust_info
                )
                
        except Exception as e:
            self.logger.error(f"Failed to adjust stop-loss: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _recreate_stop_loss(self, symbol: str, qty: int, side: OrderSide, 
                                  stop_price: float, adjust_info: Dict) -> Dict:
        """Cancel existing stop and create new one"""
        try:
            # Get tracking data
            tracked = self.position_tracker.get_position(symbol)
            
            # Cancel existing stop
            if tracked and 'stop_order_id' in tracked:
                try:
                    self.client.cancel_order_by_id(tracked['stop_order_id'])
                    await asyncio.sleep(1)
                except:
                    pass
            
            # Create new stop order
            new_stop_data = StopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce.GTC,
                stop_price=stop_price,
                extended_hours=ENABLE_EXTENDED_HOURS
            )
            
            new_stop = await self.execute_with_retry(
                self.client.submit_order,
                order_data=new_stop_data
            )
            
            # Update tracking
            if tracked:
                tracked.update({
                    'stop_order_id': str(new_stop.id),
                    'stop_loss': stop_price,
                    'stop_adjusted_timestamp': datetime.now().isoformat()
                })
                self.position_tracker.track_position(symbol, tracked)
            
            return {
                'success': True,
                'action': 'ADJUST_STOP_LOSS',
                'order_id': str(new_stop.id),
                'message': f"Stop-loss recreated at {stop_price}",
                'reason': adjust_info.get('reason', 'Manual adjustment')
            }
            
        except Exception as e:
            return {'success': False, 'error': f"Failed to recreate stop: {e}"}
    
    async def _create_new_stop_loss(self, symbol: str, qty: int, side: OrderSide,
                                    stop_price: float, adjust_info: Dict) -> Dict:
        """Create new stop loss order"""
        try:
            stop_data = StopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce.GTC,
                stop_price=stop_price,
                extended_hours=ENABLE_EXTENDED_HOURS
            )
            
            stop_order = await self.execute_with_retry(
                self.client.submit_order,
                order_data=stop_data
            )
            
            # Update tracking
            tracked = self.position_tracker.get_position(symbol) or {}
            tracked.update({
                'stop_order_id': str(stop_order.id),
                'stop_loss': stop_price,
                'stop_created_timestamp': datetime.now().isoformat()
            })
            self.position_tracker.track_position(symbol, tracked)
            
            return {
                'success': True,
                'action': 'ADJUST_STOP_LOSS',
                'order_id': str(stop_order.id),
                'message': f"New stop-loss created at {stop_price}",
                'reason': adjust_info.get('reason', 'Manual adjustment')
            }
            
        except Exception as e:
            return {'success': False, 'error': f"Failed to create stop: {e}"}
    
    # ===========================
    # SCENARIO 4: TAKE PARTIAL PROFITS
    # ===========================
    
    async def handle_take_partial_profits(self, decision: Dict) -> Dict:
        """Take partial profits from existing position"""
        symbol = decision.get('symbol')
        profit_info = decision.get('take_partial_profits', {})
        
        try:
            # Get current position
            position = self.client.get_open_position(symbol)
            current_qty = int(position.qty)
            is_long = float(position.qty) > 0
            
            # Calculate quantity to sell
            if 'percentage_to_sell' in profit_info:
                qty_to_sell = int(current_qty * (profit_info['percentage_to_sell'] / 100))
            elif 'qty' in profit_info:
                qty_to_sell = profit_info['qty']
            else:
                raise ValueError("Must specify either percentage_to_sell or qty")
            
            # Ensure we don't sell more than we have
            qty_to_sell = min(qty_to_sell, current_qty)
            
            if qty_to_sell <= 0:
                return {'success': False, 'error': 'Invalid quantity to sell'}
            
            # Place limit order to take profits
            profit_order_data = LimitOrderRequest(
                symbol=symbol,
                qty=qty_to_sell,
                side=OrderSide.SELL if is_long else OrderSide.BUY,
                time_in_force=TimeInForce.GTC,
                limit_price=profit_info['limit_price'],
                order_class=OrderClass.SIMPLE,
                extended_hours=ENABLE_EXTENDED_HOURS
            )
            
            profit_order = await self.execute_with_retry(
                self.client.submit_order,
                order_data=profit_order_data
            )
            
            # Monitor for fill
            sold_qty = await self._monitor_order_fill(str(profit_order.id), max_wait=30)
            
            # Calculate remaining position
            remaining_qty = current_qty - sold_qty
            
            # Get tracking data
            tracked = self.position_tracker.get_position(symbol) or {}
            
            # Update tracking
            tracked.update({
                'partial_profit_order_id': str(profit_order.id),
                'partial_profit_qty': sold_qty,
                'partial_profit_price': profit_info['limit_price'],
                'remaining_qty': remaining_qty,
                'partial_profit_timestamp': datetime.now().isoformat()
            })
            self.position_tracker.track_position(symbol, tracked)
            
            # Adjust stop-loss for remaining position if needed
            if remaining_qty > 0 and tracked.get('stop_order_id'):
                await self._adjust_stop_for_remaining_position(
                    symbol, remaining_qty, is_long, tracked
                )
            
            return {
                'success': True,
                'action': 'TAKE_PARTIAL_PROFITS',
                'order_id': str(profit_order.id),
                'message': f"Taking {sold_qty} shares profit at {profit_info['limit_price']}",
                'actual_sold_qty': sold_qty,
                'remaining_position': remaining_qty,
                'reason': profit_info.get('reason', 'Partial profit target reached')
            }
            
        except Exception as e:
            self.logger.error(f"Failed to take partial profits: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _adjust_stop_for_remaining_position(self, symbol: str, remaining_qty: int, 
                                                  is_long: bool, tracked: Dict):
        """Adjust stop-loss order quantity for remaining position"""
        try:
            stop_id = tracked.get('stop_order_id')
            current_stop_price = tracked.get('stop_loss')
            
            if stop_id and current_stop_price:
                # Replace stop order with new quantity
                replace_request = ReplaceOrderRequest(
                    qty=remaining_qty,
                    stop_price=current_stop_price
                )
                
                await self.execute_with_retry(
                    self.client.replace_order_by_id,
                    order_id=stop_id,
                    order_data=replace_request
                )
                
                self.logger.info(f"Adjusted stop-loss quantity to {remaining_qty} for {symbol}")
                
        except Exception as e:
            self.logger.warning(f"Could not adjust stop quantity: {e}")
    
    # ===========================
    # SCENARIO 5: EXIT FULL POSITION
    # ===========================
    
    async def handle_exit_full_position(self, decision: Dict) -> Dict:
        """Exit entire position immediately with market order"""
        symbol = decision.get('symbol')
        exit_info = decision.get('exit_full_position', {})
        
        try:
            # Get current position
            position = self.client.get_open_position(symbol)
            qty = int(position.qty)
            is_long = float(position.qty) > 0
            
            # Cancel any existing orders for this symbol
            await self._cancel_all_orders_for_symbol(symbol)
            
            # Place market order to exit immediately
            exit_order_data = MarketOrderRequest(
                symbol=symbol,
                qty=abs(qty),
                side=OrderSide.SELL if is_long else OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                extended_hours=ENABLE_EXTENDED_HOURS
            )
            
            exit_order = await self.execute_with_retry(
                self.client.submit_order,
                order_data=exit_order_data
            )
            
            # Calculate P&L if we have entry price tracked
            tracked = self.position_tracker.get_position(symbol)
            pnl_info = {}
            
            if tracked and 'entry_price' in tracked:
                entry_price = tracked['entry_price']
                current_price = float(position.current_price) if hasattr(position, 'current_price') else 0
                
                if is_long:
                    pnl = (current_price - entry_price) * qty
                    pnl_percent = ((current_price - entry_price) / entry_price) * 100
                else:
                    pnl = (entry_price - current_price) * abs(qty)
                    pnl_percent = ((entry_price - current_price) / entry_price) * 100
                    
                pnl_info = {
                    'realized_pnl': round(pnl, 2),
                    'pnl_percent': round(pnl_percent, 2),
                    'entry_price': entry_price,
                    'exit_price': current_price
                }
            
            # Update and clear position tracking
            if tracked:
                tracked.update({
                    'exit_timestamp': datetime.now().isoformat(),
                    'exit_reason': exit_info.get('reason', 'Manual exit'),
                    'exit_phase': exit_info.get('exit_signal_phase'),
                    'closed': True,
                    **pnl_info
                })
                self.position_tracker.track_position(symbol, tracked)
            
            return {
                'success': True,
                'action': 'EXIT_FULL_POSITION',
                'order_id': str(exit_order.id),
                'message': f"Exiting full position: {qty} shares of {symbol}",
                'reason': exit_info.get('reason', 'Manual exit'),
                'exit_phase': exit_info.get('exit_signal_phase'),
                **pnl_info
            }
            
        except Exception as e:
            # If standard exit fails, try using close_position API
            try:
                self.client.close_position(symbol)
                return {
                    'success': True,
                    'action': 'EXIT_FULL_POSITION',
                    'message': f"Position closed using close_position API",
                    'reason': exit_info.get('reason', 'Manual exit')
                }
            except Exception as close_error:
                self.logger.error(f"Failed to exit position: {e}, {close_error}")
                return {'success': False, 'error': f"Exit failed: {e}"}
    
    async def _cancel_all_orders_for_symbol(self, symbol: str):
        """Cancel all open orders for a symbol"""
        try:
            orders = self.client.get_orders(
                filter=GetOrdersRequest(
                    status=QueryOrderStatus.OPEN,
                    symbols=[symbol]
                )
            )
            
            for order in orders:
                try:
                    self.client.cancel_order_by_id(order.id)
                    self.logger.info(f"Cancelled order {order.id} for {symbol}")
                except Exception as e:
                    self.logger.warning(f"Could not cancel order {order.id}: {e}")
                    
            # Brief delay for cancellations to process
            if orders:
                await asyncio.sleep(1)
                
        except Exception as e:
            self.logger.warning(f"Error cancelling orders: {e}")
    
    async def _monitor_order_fill(self, order_id: str, max_wait: int = 30) -> int:
        """Monitor order for fill status"""
        start_time = time.time()
        filled_qty = 0
        
        while time.time() - start_time < max_wait:
            try:
                order = self.client.get_order_by_id(order_id)
                
                if order.status == OrderStatus.FILLED:
                    filled_qty = int(order.filled_qty) if order.filled_qty else int(order.qty)
                    self.logger.info(f"Order {order_id} filled: {filled_qty} shares")
                    break
                elif order.status in [OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED]:
                    self.logger.warning(f"Order {order_id} status: {order.status}")
                    break
                elif order.status == OrderStatus.PARTIALLY_FILLED:
                    filled_qty = int(order.filled_qty) if order.filled_qty else 0
                    self.logger.info(f"Order {order_id} partially filled: {filled_qty} shares")
                
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error monitoring order {order_id}: {e}")
                await asyncio.sleep(1)
        
        return filled_qty

# ===========================
# EXECUTOR INSTANCE
# ===========================

executor = AlpacaTradingExecutor()

# ===========================
# API ENDPOINTS
# ===========================

@app.post("/trading/execute_decision")
async def execute_trading_decision(decision: dict):
    """Main endpoint routing to specific handlers"""
    try:
        primary_action = decision.get("decision", {}).get("primary_action", "NO_ACTION")
        
        action_handlers = {
            "NEW_TRADE": executor.handle_new_trade,
            "ADD_POSITION": executor.handle_add_position,
            "ADJUST_STOP_LOSS": executor.handle_adjust_stop_loss,
            "TAKE_PARTIAL_PROFITS": executor.handle_take_partial_profits,
            "EXIT_FULL_POSITION": executor.handle_exit_full_position
        }
        
        handler = action_handlers.get(primary_action)
        if handler:
            decision_data = decision.get("decision", {})
            return await handler(decision_data)
        else:
            return {
                "success": False,
                "action": "NO_ACTION",
                "message": decision.get("decision", {}).get("no_action", {}).get("reason", "No action required")
            }
            
    except Exception as e:
        logger.error(f"Error executing decision: {e}")
        raise HTTPException(status_code=500, detail=f"Error executing decision: {str(e)}")

@app.get("/account/buying_power")
async def get_account_buying_power():
    """Get account buying power"""
    try:
        account = trading_client.get_account()
        return {"buying_power": float(account.buying_power)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching buying power: {str(e)}")

@app.get("/positions")
async def get_all_positions():
    """Get all open positions"""
    try:
        positions = trading_client.get_all_positions()
        result = [
            {
                "symbol": position.symbol,
                "qty": position.qty,
                "market_value": position.market_value,
                "avg_entry_price": position.avg_entry_price,
                "unrealized_pl": position.unrealized_pl,
                "current_price": position.current_price
            }
            for position in positions
        ]
        return {"positions": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching positions: {str(e)}")

@app.get("/positions/{symbol}/orders")
async def get_position_orders(symbol: str):
    """Get all orders for a specific position"""
    try:
        # Get position info
        try:
            position = trading_client.get_open_position(symbol)
            position_info = {
                "qty": position.qty,
                "avg_entry_price": position.avg_entry_price,
                "market_value": position.market_value,
                "unrealized_pl": position.unrealized_pl
            }
        except:
            position_info = None
        
        # Get all open orders
        all_orders = trading_client.get_orders(
            filter=GetOrdersRequest(
                status=QueryOrderStatus.OPEN,
                symbols=[symbol]
            )
        )
        
        orders_info = []
        stop_orders = []
        limit_orders = []
        
        for order in all_orders:
            order_data = {
                "id": str(order.id),
                "side": str(order.side),
                "qty": order.qty,
                "order_type": str(order.order_type),
                "order_class": str(order.order_class),
                "status": str(order.status),
                "stop_price": order.stop_price,
                "limit_price": order.limit_price,
                "created_at": str(order.created_at) if order.created_at else None
            }
            
            orders_info.append(order_data)
            
            if order.stop_price is not None:
                stop_orders.append(order_data)
            elif order.limit_price is not None and order.stop_price is None:
                limit_orders.append(order_data)
        
        # Get tracking data
        tracked = position_tracker.get_position(symbol)
        
        return {
            "symbol": symbol,
            "position": position_info,
            "all_orders": orders_info,
            "stop_orders": stop_orders,
            "limit_orders": limit_orders,
            "order_count": len(orders_info),
            "tracking_data": tracked
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting position orders: {str(e)}")

@app.get("/allpositions")
async def get_all_positions():
    try:
        # Fetch all open positions from Alpaca
        positions = trading_client.get_all_positions()
        print("All positions fetched:", positions)
        # Extract requested fields for each position
        result = [
            {
                "Symbol": position.symbol if position.symbol else None,
                "Quantity": position.qty if position.qty else None,
                "Market Value": position.market_value if position.market_value else None,
                "Asset Id": str(position.asset_id) if position.asset_id else None,
                "Avg Entry Price": position.avg_entry_price if position.avg_entry_price else None,
                "Side": str(position.side) if position.side else None,
                "Unrealized Pl": position.unrealized_pl if position.unrealized_pl else None,
                "Unrealized Pl Pc": position.unrealized_plpc if position.unrealized_plpc else None,
                "Unrealized Intraday Pl": position.unrealized_intraday_pl if position.unrealized_intraday_pl else None,
                "Unrealized Intraday Pl Pc": position.unrealized_intraday_plpc if position.unrealized_intraday_plpc else None,
                "Current Price": position.current_price if position.current_price else None,
                "Last Day Price": position.lastday_price if position.lastday_price else None,
                "Change Today": position.change_today if position.change_today else None
            }
            for position in positions
        ]        

        return {"positions": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching positions: {str(e)}")

@app.get("/accountinfo")
async def account_info():
    accountInfo = trading_client.get_account()
    return {"getAccount": accountInfo}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        account = trading_client.get_account()
        return {
            "status": "healthy",
            "paper_trading": True,
            "account_id": account.id,
            "buying_power": float(account.buying_power)
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
