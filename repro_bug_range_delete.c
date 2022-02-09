// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#include "splinterdb/platform_public.h"
#include "splinterdb/splinterdb_kv.h"
#include "tests/unit/unit_tests.h"
#include "tests/functional/random.h"

#define KEY_SIZE   (MAX_KEY_SIZE - 8)
#define VALUE_SIZE 200

#define ASSERT_EQUAL(a, b) platform_assert((a) == (b))

static uint32
naive_range_delete(const splinterdb_kv *kvsb,
                   char                *start_key,
                   size_t               start_key_len,
                   uint32               count)
{
   fprintf(stderr, "\tcollecting keys to delete...\n");
   char *keys_to_delete = calloc(count, KEY_SIZE);

   splinterdb_kv_iterator *it;
   int rc = splinterdb_kv_iter_init(kvsb, &it, start_key, start_key_len);
   ASSERT_EQUAL(0, rc);

   const char *key;
   const char *val;
   size_t      key_len, val_len;
   uint32      num_found = 0;
   for (; splinterdb_kv_iter_valid(it); splinterdb_kv_iter_next(it)) {
      splinterdb_kv_iter_get_current(it, &key, &key_len, &val, &val_len);
      ASSERT_EQUAL(KEY_SIZE, key_len);
      memcpy(keys_to_delete + num_found * KEY_SIZE, key, KEY_SIZE);
      num_found++;
      if (num_found >= count) {
         break;
      }
   }
   rc = splinterdb_kv_iter_status(it);
   ASSERT_EQUAL(0, rc);
   splinterdb_kv_iter_deinit(&it);

   fprintf(stderr, "\tdeleting collected keys...\n");
   for (uint32 i = 0; i < num_found; i++) {
      char *key_to_delete = keys_to_delete + i * KEY_SIZE;
      splinterdb_kv_delete(kvsb, key_to_delete, KEY_SIZE);
   }

   free(keys_to_delete);
   return num_found;
}

static void
uniform_random_inserts(const splinterdb_kv *kvsb,
                       uint32               count,
                       random_state        *rand_state)
{
   char key_buffer[KEY_SIZE]     = {0};
   char value_buffer[VALUE_SIZE] = {0};

   for (uint32 i = 0; i < count; i++) {
      random_bytes(rand_state, key_buffer, KEY_SIZE);
      random_bytes(rand_state, value_buffer, VALUE_SIZE);
      int rc = splinterdb_kv_insert(
         kvsb, key_buffer, KEY_SIZE, value_buffer, VALUE_SIZE);
      ASSERT_EQUAL(0, rc);
   }
}


int
main()
{
   splinterdb_kv_cfg cfg = (splinterdb_kv_cfg){
      .filename       = "db",
      .cache_size     = 3 * Giga,
      .disk_size      = 128 * Giga,
      .max_key_size   = KEY_SIZE,
      .max_value_size = VALUE_SIZE,
   };

   splinterdb_kv *kvsb;

   int rc = splinterdb_kv_create(&cfg, &kvsb);
   ASSERT_EQUAL(0, rc);

   random_state rand_state;
   random_init(&rand_state, 42, 0);

   const uint32 num_inserts = 5 * 1000 * 1000;
   fprintf(stderr, "loading data...\n");
   uniform_random_inserts(kvsb, num_inserts, &rand_state);
   fprintf(stderr, "loaded %u k/v pairs\n", num_inserts);

   uint32 num_rounds = 5;
   for (uint32 round = 0; round < num_rounds; round++) {
      fprintf(stderr, "range delete round %d...\n", round);
      char start_key[4];
      random_bytes(&rand_state, start_key, sizeof(start_key));
      const uint32 num_to_delete = num_inserts / num_rounds;

      uint32 num_deleted =
         naive_range_delete(kvsb, start_key, sizeof(start_key), num_to_delete);
      fprintf(stderr, "\tdeleted %u k/v pairs\n", num_deleted);
   }

   splinterdb_kv_close(kvsb);
}
