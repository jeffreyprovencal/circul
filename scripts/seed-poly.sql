-- Seed: two completed processor_sale rows for processor 1 (rePATRN) → converter 2 (Poly Recycling AG)
INSERT INTO pending_transactions (
  transaction_type, processor_id, converter_id, material_type,
  gross_weight_kg, price_per_kg, total_price, status,
  photos_required, photos_submitted, dispatch_approved, photo_urls,
  created_at, updated_at
) VALUES
  ('processor_sale', 1, 2, 'PET', 500, 6.50, 3250.00, 'completed',
   false, false, true, '[]'::jsonb,
   NOW() - INTERVAL '10 days', NOW() - INTERVAL '8 days'),
  ('processor_sale', 1, 2, 'PET', 320, 6.20, 1984.00, 'completed',
   false, false, true, '[]'::jsonb,
   NOW() - INTERVAL '11 days', NOW() - INTERVAL '9 days');
