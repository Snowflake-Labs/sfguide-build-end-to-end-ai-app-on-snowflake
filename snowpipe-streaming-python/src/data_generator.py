import random
import uuid
from datetime import datetime, timedelta
from typing import List
from decimal import Decimal, ROUND_HALF_UP
from models import Customer, Order, OrderItem


class DataGenerator:
    FIRST_NAMES = [
        "John", "Sarah", "Michael", "Emily", "David", "Jessica", "Chris", "Ashley",
        "Matt", "Amanda", "Ryan", "Lauren", "Kevin", "Nicole", "Brian", "Rachel",
        "Tyler", "Megan", "Josh", "Katie"
    ]
    
    LAST_NAMES = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
        "Thomas", "Taylor", "Moore", "Jackson", "Martin"
    ]
    
    STREETS = [
        "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine Rd", "Elm St",
        "Washington Blvd", "Lake View Dr", "Mountain Way", "Summit Trail"
    ]
    
    CITIES = [
        "Denver", "Salt Lake City", "Boulder", "Aspen", "Park City", "Jackson",
        "Telluride", "Steamboat Springs", "Vail", "Breckenridge", "Mammoth Lakes",
        "Tahoe City", "Whistler", "Banff", "Portland"
    ]
    
    STATES = ["CO", "UT", "WY", "CA", "WA", "OR", "MT", "ID", "NV", "BC"]
    
    SEGMENTS = ["Premium", "Standard", "Basic"]
    
    ORDER_STATUSES = ["Completed", "Pending", "Shipped", "Cancelled", "Processing"]
    
    PRODUCT_NAMES = [
        "Powder Skis", "All-Mountain Skis", "Freestyle Snowboard", "Freeride Snowboard",
        "Ski Boots", "Snowboard Boots", "Ski Poles", "Ski Goggles", "Snowboard Bindings", "Ski Helmet"
    ]
    
    PRODUCT_CATEGORIES = [
        "Skis", "Skis", "Snowboards", "Snowboards",
        "Boots", "Boots", "Accessories", "Accessories", "Accessories", "Accessories"
    ]

    @staticmethod
    def random_customer_id(max_customer_id: int) -> int:
        if max_customer_id <= 0:
            raise ValueError("Max customer ID must be positive")
        return random.randint(1, max_customer_id)

    @staticmethod
    def random_customer_id_in_range(min_customer_id: int, max_customer_id: int) -> int:
        if min_customer_id <= 0 or max_customer_id < min_customer_id:
            raise ValueError(f"Invalid customer ID range: {min_customer_id}-{max_customer_id}")
        return random.randint(min_customer_id, max_customer_id)

    @staticmethod
    def generate_customer(customer_id: int) -> Customer:
        first_name = random.choice(DataGenerator.FIRST_NAMES)
        last_name = random.choice(DataGenerator.LAST_NAMES)
        email = f"customer{customer_id}@email.com"
        phone = f"555-{random.randint(100, 999):03d}-{random.randint(1000, 9999):04d}"
        address = f"{random.randint(100, 9999)} {random.choice(DataGenerator.STREETS)}"
        city = random.choice(DataGenerator.CITIES)
        state = random.choice(DataGenerator.STATES)
        zip_code = f"{random.randint(10000, 99999):05d}"
        reg_date = datetime.now() - timedelta(days=random.randint(1, 1825))
        customer_segment = random.choice(DataGenerator.SEGMENTS)
        
        return Customer(
            customer_id=customer_id,
            first_name=first_name,
            last_name=last_name,
            email=email,
            phone=phone,
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            registration_date=reg_date.strftime("%Y-%m-%d"),
            customer_segment=customer_segment,
        )

    @staticmethod
    def generate_order(customer_id: int, customer_segment: str) -> Order:
        order_id = str(uuid.uuid4())
        
        # Generate orders within the last 7 days (streaming = fresh data arriving now)
        days_ago = random.randint(0, 7)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        order_date = datetime.now() - timedelta(days=days_ago, hours=hour, minutes=minute, seconds=second)
        
        # Weight order statuses realistically (more completed, fewer cancelled)
        rand = random.random()
        if rand < 0.65:  # 65% completed
            order_status = "Completed"
        elif rand < 0.80:  # 15% shipped
            order_status = "Shipped"
        elif rand < 0.90:  # 10% processing
            order_status = "Processing"
        elif rand < 0.97:  # 7% pending
            order_status = "Pending"
        else:  # 3% cancelled
            order_status = "Cancelled"
        
        # Segment-based order amounts and discounts
        if customer_segment == "Premium":
            # Premium: $500-$3000, rarely discounted (10% chance, 5-10% off)
            total_amount = DataGenerator._random_decimal(500.0, 3000.0)
            discount_percent = (
                Decimal(random.randint(5, 10))
                if random.randint(1, 10) > 9
                else Decimal(0)
            )
        elif customer_segment == "Standard":
            # Standard: $100-$800, moderate discounts (40% chance, 5-20% off)
            total_amount = DataGenerator._random_decimal(100.0, 800.0)
            discount_percent = (
                Decimal(random.randint(5, 20))
                if random.randint(1, 10) > 6
                else Decimal(0)
            )
        else:  # Basic
            # Basic: $20-$300, frequent discounts (50% chance, 10-30% off)
            total_amount = DataGenerator._random_decimal(20.0, 300.0)
            discount_percent = (
                Decimal(random.randint(10, 30))
                if random.randint(1, 10) > 5
                else Decimal(0)
            )
        
        shipping_cost = DataGenerator._random_decimal(5.0, 50.0)
        
        return Order(
            order_id=order_id,
            customer_id=customer_id,
            order_date=order_date.strftime("%Y-%m-%d %H:%M:%S"),
            order_status=order_status,
            total_amount=float(total_amount),
            discount_percent=float(discount_percent),
            shipping_cost=float(shipping_cost),
        )

    @staticmethod
    def generate_order_items(order_id: str, customer_segment: str, count: int) -> List[OrderItem]:
        items = []
        for i in range(count):
            order_item_id = str(uuid.uuid4())
            
            product_index = random.randint(0, len(DataGenerator.PRODUCT_NAMES) - 1)
            product_id = 1001 + product_index
            product_name = DataGenerator.PRODUCT_NAMES[product_index]
            product_category = DataGenerator.PRODUCT_CATEGORIES[product_index]
            
            # Segment-based quantity and pricing
            if customer_segment == "Premium":
                quantity = random.randint(2, 5)
                unit_price = DataGenerator._random_decimal(150.0, 500.0)
            elif customer_segment == "Standard":
                quantity = random.randint(1, 3)
                unit_price = DataGenerator._random_decimal(50.0, 250.0)
            else:  # Basic
                quantity = random.randint(1, 2)
                unit_price = DataGenerator._random_decimal(10.0, 100.0)
            
            line_total = unit_price * Decimal(quantity)
            line_total = line_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            
            items.append(
                OrderItem(
                    order_item_id=order_item_id,
                    order_id=order_id,
                    product_id=product_id,
                    product_name=product_name,
                    product_category=product_category,
                    quantity=quantity,
                    unit_price=float(unit_price),
                    line_total=float(line_total),
                )
            )
        return items

    @staticmethod
    def _random_decimal(min_val: float, max_val: float) -> Decimal:
        value = min_val + (max_val - min_val) * random.random()
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    @staticmethod
    def random_item_count(customer_segment: str) -> int:
        if customer_segment == "Premium":
            return random.randint(3, 8)  # 3-8 items
        elif customer_segment == "Standard":
            return random.randint(2, 5)  # 2-5 items
        else:  # Basic
            return random.randint(1, 3)  # 1-3 items
