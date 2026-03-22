-- Seed: two completed aggregator_sale rows for aggregator 9 → processor 1 (Repatrn demo)
INSERT INTO pending_transactions (
  transaction_type, aggregator_id, processor_id, material_type,
  gross_weight_kg, price_per_kg, total_price, status,
  photos_required, photos_submitted, dispatch_approved, photo_urls,
  created_at, updated_at
) VALUES
  ('aggregator_sale', 9, 1, 'PET', 320, 2.20, 704.00, 'completed',
   false, false, true, '{}',
   NOW() - INTERVAL '14 days', NOW() - INTERVAL '12 days'),
  ('aggregator_sale', 9, 1, 'PET', 180, 2.00, 360.00, 'completed',
   false, false, true, '{}',
   NOW() - INTERVAL '15 days', NOW() - INTERVAL '13 days');
