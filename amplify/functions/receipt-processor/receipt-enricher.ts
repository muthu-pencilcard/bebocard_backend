export interface ReceiptItem {
  name: string;
  price: number;
  quantity: number;
  originalPrice?: number;
  category?: string;
}

export interface EnrichmentResult {
  totalSavings: number;
  itemCount: number;
  categories: Record<string, number>; // Category name -> Total spend in that category
  taxTotal?: number;
}

const CATEGORY_MAP: Record<string, string> = {
  // Grocery
  'milk': 'grocery',
  'bread': 'grocery',
  'egg': 'grocery',
  'water': 'grocery',
  'fruit': 'grocery',
  'veg': 'grocery',
  'snack': 'grocery',
  // Fuel
  'petrol': 'fuel',
  'diesel': 'fuel',
  'unleaded': 'fuel',
  // Health
  'pharmacy': 'health',
  'medicine': 'health',
  'vitamin': 'health',
  // Dining
  'coffee': 'dining',
  'burger': 'dining',
  'pizza': 'dining',
  'restaurant': 'dining',
};

export function enrichReceipt(items: any[]): EnrichmentResult {
  let totalSavings = 0;
  let itemCount = 0;
  let taxTotal = 0;
  const categories: Record<string, number> = {};

  for (const rawItem of items) {
    const item = rawItem as ReceiptItem;
    const qty = item.quantity ?? 1;
    const price = (item.price ?? 0) * qty;

    itemCount += qty;

    if (item.originalPrice && item.originalPrice > item.price) {
      totalSavings += (item.originalPrice - item.price) * qty;
    }

    // Keyword matching for category
    let category = item.category?.toLowerCase() || 'other';
    if (category === 'other') {
      const name = item.name.toLowerCase();
      for (const [kw, cat] of Object.entries(CATEGORY_MAP)) {
        if (name.includes(kw)) {
          category = cat;
          break;
        }
      }
    }

    categories[category] = (categories[category] ?? 0) + price;
  }

  return {
    totalSavings: Math.round(totalSavings * 100) / 100,
    itemCount,
    categories,
    taxTotal: 0, // Injected by processor if supplier data is present
  };
}
