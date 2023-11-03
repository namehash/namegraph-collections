
#line 1 "sql_parser.rl"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

//#define BUFFER_SIZE 32576
long BUFFER_SIZE = 1024;
//#define DEBUG
#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))


#line 77 "sql_parser.rl"



#line 23 "sql_parser.c"
static const int wikiparser_start = 0;
static const int wikiparser_first_final = 0;
static const int wikiparser_error = -1;

static const int wikiparser_en_main = 0;


#line 80 "sql_parser.rl"

int main( int argc, char **argv )
{
  int cs, res = 0;
  long id_start_position, id_len, text_start_position, text_end_position, count = 0;
  int page_size = sysconf(_SC_PAGE_SIZE);
  int file;
  char * p;
  struct stat file_info;
  long file_size,pages_count;
  int id_seen = 0;
  char * name_start;
  char * element_start;
  int field_counter = 0;
  int field_mask = 0;
  int values_flag = 0;
  char buffer[BUFFER_SIZE];

  if ( argc >= 2 ) {
    if(strlen(argv[1]) > 5){
      // assume the first arg is a file path
      char *file_name = argv[1];

      for(int i =0; i < argc - 2; i++){
        int field_id = atoi(argv[i+2]);
        field_mask |= (1 << field_id);
      }
      file = open(file_name,O_RDONLY);
      if(file == -1){
        printf("File error %i\n",file);
        return file;
      }
      fstat(file,&file_info);
      file_size = file_info.st_size;
      pages_count = file_size / page_size + (file_size % page_size == 0 ? 0 : 1);
      p = (char*)mmap(NULL,pages_count * page_size,PROT_READ,MAP_SHARED,file,0);
      if(p == MAP_FAILED){
        printf("Mapping failed\n");
        return -1;
      }
      char *start = p;
      char *pe = p + file_size + 1;
      //%% write exec;
      munmap(p,pages_count * page_size);
      close(file);
    } else {

      for(int i =0; i < argc - 1; i++){
        int field_id = atoi(argv[i+1]);
        field_mask |= (1 << field_id);
      }
      // assume readin from stdin
      long bytes_read;
      long remainder = 0;

      while (1) {
        p = buffer;
        bytes_read = fread(p + remainder, 1,  BUFFER_SIZE - remainder, stdin);
        if(bytes_read == 0)
          break;
#ifdef DEBUG
        printf("\nBuffer %lu %lu\n", remainder, bytes_read);
#endif
        char *start = p;
        bytes_read += remainder;
        char *pe = p + bytes_read;
#ifdef DEBUG
        printf("================= START ==========\n");
        for (long i = 0; i < bytes_read; i++) {
          putchar(p[i]);
        }
        printf("================= END ==========\n");
#endif

        // Process the data in the buffer

        int state = 0;
        remainder = 0;

        long j;
        for (j = bytes_read-1; j > 0; j--) {
            //putchar(p[i]);
            //printf("%d\n", i);
            if (p[j] == ',') {
              state = 1;
            } else if (p[j] == ')' && state == 1) {
              pe = p + j + 1;
              remainder = bytes_read - j - 1;
              break;
            } else {
              state = 0;
            }
        }

        if(remainder >= BUFFER_SIZE){
          printf("Remainder larger than buffer!!!%lu\n", remainder);
        }
        
#line 130 "sql_parser.c"
	{
	cs = wikiparser_start;
	}

#line 178 "sql_parser.rl"
        
#line 137 "sql_parser.c"
	{
	if ( p == pe )
		goto _test_eof;
	switch ( cs )
	{
tr11:
#line 39 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start - 2), element_start + 1 ); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st0;
tr15:
#line 33 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start + 1), element_start - 1 ); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st0;
tr74:
#line 28 "sql_parser.rl"
	{ name_start = p + 2; }
	goto st0;
tr152:
#line 46 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("\t"); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st0;
st0:
	if ( ++p == pe )
		goto _test_eof0;
case 0:
#line 196 "sql_parser.c"
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
	}
	goto st0;
st1:
	if ( ++p == pe )
		goto _test_eof1;
case 1:
	switch( (*p) ) {
		case 39: goto tr3;
		case 40: goto st1;
		case 73: goto st8;
		case 78: goto tr5;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr4;
	goto st0;
tr3:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st2;
tr108:
#line 39 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start - 2), element_start + 1 ); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st2;
tr113:
#line 33 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start + 1), element_start - 1 ); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st2;
tr137:
#line 28 "sql_parser.rl"
	{ name_start = p + 2; }
	goto st2;
tr131:
#line 46 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("\t"); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st2;
st2:
	if ( ++p == pe )
		goto _test_eof2;
case 2:
#line 272 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 92: goto st94;
	}
	goto st2;
st3:
	if ( ++p == pe )
		goto _test_eof3;
case 3:
	switch( (*p) ) {
		case 40: goto st1;
		case 41: goto tr11;
		case 44: goto tr12;
		case 73: goto st8;
	}
	goto st0;
tr12:
#line 39 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start - 2), element_start + 1 ); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st4;
tr16:
#line 33 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start + 1), element_start - 1 ); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st4;
tr153:
#line 46 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("\t"); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st4;
st4:
	if ( ++p == pe )
		goto _test_eof4;
case 4:
#line 328 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st2;
		case 40: goto st1;
		case 73: goto st8;
		case 78: goto st114;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st5;
	goto st0;
tr4:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st5;
st5:
	if ( ++p == pe )
		goto _test_eof5;
case 5:
#line 346 "sql_parser.c"
	switch( (*p) ) {
		case 40: goto st1;
		case 41: goto tr15;
		case 44: goto tr16;
		case 46: goto st6;
		case 73: goto st8;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st5;
	goto st0;
st6:
	if ( ++p == pe )
		goto _test_eof6;
case 6:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st7;
	goto st0;
st7:
	if ( ++p == pe )
		goto _test_eof7;
case 7:
	switch( (*p) ) {
		case 40: goto st1;
		case 41: goto tr15;
		case 44: goto tr16;
		case 73: goto st8;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st7;
	goto st0;
st8:
	if ( ++p == pe )
		goto _test_eof8;
case 8:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 78: goto st9;
	}
	goto st0;
st9:
	if ( ++p == pe )
		goto _test_eof9;
case 9:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 83: goto st10;
	}
	goto st0;
st10:
	if ( ++p == pe )
		goto _test_eof10;
case 10:
	switch( (*p) ) {
		case 40: goto st1;
		case 69: goto st11;
		case 73: goto st8;
	}
	goto st0;
st11:
	if ( ++p == pe )
		goto _test_eof11;
case 11:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 82: goto st12;
	}
	goto st0;
st12:
	if ( ++p == pe )
		goto _test_eof12;
case 12:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 84: goto st13;
	}
	goto st0;
st13:
	if ( ++p == pe )
		goto _test_eof13;
case 13:
	switch( (*p) ) {
		case 32: goto st14;
		case 40: goto st1;
		case 73: goto st8;
	}
	goto st0;
st14:
	if ( ++p == pe )
		goto _test_eof14;
case 14:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st15;
	}
	goto st0;
st15:
	if ( ++p == pe )
		goto _test_eof15;
case 15:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 78: goto st16;
	}
	goto st0;
st16:
	if ( ++p == pe )
		goto _test_eof16;
case 16:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 83: goto st10;
		case 84: goto st17;
	}
	goto st0;
st17:
	if ( ++p == pe )
		goto _test_eof17;
case 17:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 79: goto st18;
	}
	goto st0;
st18:
	if ( ++p == pe )
		goto _test_eof18;
case 18:
	switch( (*p) ) {
		case 32: goto st19;
		case 40: goto st1;
		case 73: goto st8;
	}
	if ( 9 <= (*p) && (*p) <= 13 )
		goto st19;
	goto st0;
st19:
	if ( ++p == pe )
		goto _test_eof19;
case 19:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 96: goto st20;
	}
	goto st0;
tr43:
#line 39 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start - 2), element_start + 1 ); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st20;
tr47:
#line 33 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start + 1), element_start - 1 ); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st20;
tr68:
#line 28 "sql_parser.rl"
	{ name_start = p + 2; }
	goto st20;
tr78:
#line 46 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("\t"); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st20;
st20:
	if ( ++p == pe )
		goto _test_eof20;
case 20:
#line 556 "sql_parser.c"
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 96: goto tr33;
	}
	goto st20;
st21:
	if ( ++p == pe )
		goto _test_eof21;
case 21:
	switch( (*p) ) {
		case 39: goto tr34;
		case 40: goto st21;
		case 73: goto st28;
		case 78: goto tr36;
		case 96: goto tr33;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr35;
	goto st20;
tr34:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st22;
tr83:
#line 39 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start - 2), element_start + 1 ); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st22;
tr88:
#line 33 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start + 1), element_start - 1 ); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st22;
tr143:
#line 28 "sql_parser.rl"
	{ name_start = p + 2; }
	goto st22;
tr147:
#line 46 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("\t"); 
    }
    field_counter += 1; 
  }
#line 52 "sql_parser.rl"
	{ 
    if(values_flag == 1) {
      printf("\n"); 
    }
  }
	goto st22;
st22:
	if ( ++p == pe )
		goto _test_eof22;
case 22:
#line 634 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st23:
	if ( ++p == pe )
		goto _test_eof23;
case 23:
	switch( (*p) ) {
		case 40: goto st21;
		case 41: goto tr43;
		case 44: goto tr44;
		case 73: goto st28;
		case 96: goto tr33;
	}
	goto st20;
tr44:
#line 39 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start - 2), element_start + 1 ); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st24;
tr48:
#line 33 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start + 1), element_start - 1 ); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st24;
tr79:
#line 46 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("\t"); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st24;
st24:
	if ( ++p == pe )
		goto _test_eof24;
case 24:
#line 692 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st22;
		case 40: goto st21;
		case 73: goto st28;
		case 78: goto st52;
		case 96: goto tr33;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st25;
	goto st20;
tr35:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st25;
st25:
	if ( ++p == pe )
		goto _test_eof25;
case 25:
#line 711 "sql_parser.c"
	switch( (*p) ) {
		case 40: goto st21;
		case 41: goto tr47;
		case 44: goto tr48;
		case 46: goto st26;
		case 73: goto st28;
		case 96: goto tr33;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st25;
	goto st20;
st26:
	if ( ++p == pe )
		goto _test_eof26;
case 26:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 96: goto tr33;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st27;
	goto st20;
st27:
	if ( ++p == pe )
		goto _test_eof27;
case 27:
	switch( (*p) ) {
		case 40: goto st21;
		case 41: goto tr47;
		case 44: goto tr48;
		case 73: goto st28;
		case 96: goto tr33;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st27;
	goto st20;
st28:
	if ( ++p == pe )
		goto _test_eof28;
case 28:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 78: goto st29;
		case 96: goto tr33;
	}
	goto st20;
st29:
	if ( ++p == pe )
		goto _test_eof29;
case 29:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 83: goto st30;
		case 96: goto tr33;
	}
	goto st20;
st30:
	if ( ++p == pe )
		goto _test_eof30;
case 30:
	switch( (*p) ) {
		case 40: goto st21;
		case 69: goto st31;
		case 73: goto st28;
		case 96: goto tr33;
	}
	goto st20;
st31:
	if ( ++p == pe )
		goto _test_eof31;
case 31:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 82: goto st32;
		case 96: goto tr33;
	}
	goto st20;
st32:
	if ( ++p == pe )
		goto _test_eof32;
case 32:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 84: goto st33;
		case 96: goto tr33;
	}
	goto st20;
st33:
	if ( ++p == pe )
		goto _test_eof33;
case 33:
	switch( (*p) ) {
		case 32: goto st34;
		case 40: goto st21;
		case 73: goto st28;
		case 96: goto tr33;
	}
	goto st20;
st34:
	if ( ++p == pe )
		goto _test_eof34;
case 34:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st35;
		case 96: goto tr33;
	}
	goto st20;
st35:
	if ( ++p == pe )
		goto _test_eof35;
case 35:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 78: goto st36;
		case 96: goto tr33;
	}
	goto st20;
st36:
	if ( ++p == pe )
		goto _test_eof36;
case 36:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 83: goto st30;
		case 84: goto st37;
		case 96: goto tr33;
	}
	goto st20;
st37:
	if ( ++p == pe )
		goto _test_eof37;
case 37:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 79: goto st38;
		case 96: goto tr33;
	}
	goto st20;
st38:
	if ( ++p == pe )
		goto _test_eof38;
case 38:
	switch( (*p) ) {
		case 32: goto st39;
		case 40: goto st21;
		case 73: goto st28;
		case 96: goto tr33;
	}
	if ( 9 <= (*p) && (*p) <= 13 )
		goto st39;
	goto st20;
st39:
	if ( ++p == pe )
		goto _test_eof39;
case 39:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 96: goto tr62;
	}
	goto st20;
tr62:
#line 57 "sql_parser.rl"
	{ values_flag = 1; }
	goto st40;
st40:
	if ( ++p == pe )
		goto _test_eof40;
case 40:
#line 890 "sql_parser.c"
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 86: goto st41;
		case 96: goto tr33;
	}
	goto st20;
st41:
	if ( ++p == pe )
		goto _test_eof41;
case 41:
	switch( (*p) ) {
		case 40: goto st21;
		case 65: goto st42;
		case 73: goto st28;
		case 96: goto tr33;
	}
	goto st20;
st42:
	if ( ++p == pe )
		goto _test_eof42;
case 42:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 76: goto st43;
		case 96: goto tr33;
	}
	goto st20;
st43:
	if ( ++p == pe )
		goto _test_eof43;
case 43:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 85: goto st44;
		case 96: goto tr33;
	}
	goto st20;
st44:
	if ( ++p == pe )
		goto _test_eof44;
case 44:
	switch( (*p) ) {
		case 40: goto st21;
		case 69: goto st45;
		case 73: goto st28;
		case 96: goto tr33;
	}
	goto st20;
st45:
	if ( ++p == pe )
		goto _test_eof45;
case 45:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 83: goto tr68;
		case 96: goto tr33;
	}
	goto st20;
tr33:
#line 57 "sql_parser.rl"
	{ values_flag = 1; }
	goto st46;
st46:
	if ( ++p == pe )
		goto _test_eof46;
case 46:
#line 961 "sql_parser.c"
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 86: goto st47;
	}
	goto st0;
st47:
	if ( ++p == pe )
		goto _test_eof47;
case 47:
	switch( (*p) ) {
		case 40: goto st1;
		case 65: goto st48;
		case 73: goto st8;
	}
	goto st0;
st48:
	if ( ++p == pe )
		goto _test_eof48;
case 48:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 76: goto st49;
	}
	goto st0;
st49:
	if ( ++p == pe )
		goto _test_eof49;
case 49:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 85: goto st50;
	}
	goto st0;
st50:
	if ( ++p == pe )
		goto _test_eof50;
case 50:
	switch( (*p) ) {
		case 40: goto st1;
		case 69: goto st51;
		case 73: goto st8;
	}
	goto st0;
st51:
	if ( ++p == pe )
		goto _test_eof51;
case 51:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 83: goto tr74;
	}
	goto st0;
tr36:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st52;
st52:
	if ( ++p == pe )
		goto _test_eof52;
case 52:
#line 1026 "sql_parser.c"
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 85: goto st53;
		case 96: goto tr33;
	}
	goto st20;
st53:
	if ( ++p == pe )
		goto _test_eof53;
case 53:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 76: goto st54;
		case 96: goto tr33;
	}
	goto st20;
st54:
	if ( ++p == pe )
		goto _test_eof54;
case 54:
	switch( (*p) ) {
		case 40: goto st21;
		case 73: goto st28;
		case 76: goto st55;
		case 96: goto tr33;
	}
	goto st20;
st55:
	if ( ++p == pe )
		goto _test_eof55;
case 55:
	switch( (*p) ) {
		case 40: goto st21;
		case 41: goto tr78;
		case 44: goto tr79;
		case 73: goto st28;
		case 96: goto tr33;
	}
	goto st20;
st56:
	if ( ++p == pe )
		goto _test_eof56;
case 56:
	switch( (*p) ) {
		case 39: goto tr80;
		case 40: goto st56;
		case 73: goto st62;
		case 78: goto tr82;
		case 92: goto st74;
		case 96: goto tr42;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr81;
	goto st22;
tr80:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st57;
st57:
	if ( ++p == pe )
		goto _test_eof57;
case 57:
#line 1091 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 41: goto tr83;
		case 44: goto tr84;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
tr84:
#line 39 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start - 2), element_start + 1 ); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st58;
tr89:
#line 33 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start + 1), element_start - 1 ); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st58;
tr148:
#line 46 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("\t"); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st58;
st58:
	if ( ++p == pe )
		goto _test_eof58;
case 58:
#line 1139 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st57;
		case 40: goto st56;
		case 73: goto st62;
		case 78: goto st110;
		case 92: goto st74;
		case 96: goto tr42;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st59;
	goto st22;
tr81:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st59;
st59:
	if ( ++p == pe )
		goto _test_eof59;
case 59:
#line 1159 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 41: goto tr88;
		case 44: goto tr89;
		case 46: goto st60;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st59;
	goto st22;
st60:
	if ( ++p == pe )
		goto _test_eof60;
case 60:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st61;
	goto st22;
st61:
	if ( ++p == pe )
		goto _test_eof61;
case 61:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 41: goto tr88;
		case 44: goto tr89;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st61;
	goto st22;
st62:
	if ( ++p == pe )
		goto _test_eof62;
case 62:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 78: goto st63;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st63:
	if ( ++p == pe )
		goto _test_eof63;
case 63:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 83: goto st64;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st64:
	if ( ++p == pe )
		goto _test_eof64;
case 64:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 69: goto st65;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st65:
	if ( ++p == pe )
		goto _test_eof65;
case 65:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 82: goto st66;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st66:
	if ( ++p == pe )
		goto _test_eof66;
case 66:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 84: goto st67;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st67:
	if ( ++p == pe )
		goto _test_eof67;
case 67:
	switch( (*p) ) {
		case 32: goto st68;
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st68:
	if ( ++p == pe )
		goto _test_eof68;
case 68:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st69;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st69:
	if ( ++p == pe )
		goto _test_eof69;
case 69:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 78: goto st70;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st70:
	if ( ++p == pe )
		goto _test_eof70;
case 70:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 83: goto st64;
		case 84: goto st71;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st71:
	if ( ++p == pe )
		goto _test_eof71;
case 71:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 79: goto st72;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st72:
	if ( ++p == pe )
		goto _test_eof72;
case 72:
	switch( (*p) ) {
		case 32: goto st73;
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	if ( 9 <= (*p) && (*p) <= 13 )
		goto st73;
	goto st22;
st73:
	if ( ++p == pe )
		goto _test_eof73;
case 73:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr103;
	}
	goto st22;
st74:
	if ( ++p == pe )
		goto _test_eof74;
case 74:
	switch( (*p) ) {
		case 39: goto st57;
		case 40: goto st56;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
tr42:
#line 57 "sql_parser.rl"
	{ values_flag = 1; }
	goto st75;
st75:
	if ( ++p == pe )
		goto _test_eof75;
case 75:
#line 1380 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 86: goto st99;
		case 92: goto st94;
	}
	goto st2;
st76:
	if ( ++p == pe )
		goto _test_eof76;
case 76:
	switch( (*p) ) {
		case 39: goto tr105;
		case 40: goto st76;
		case 73: goto st82;
		case 78: goto tr107;
		case 92: goto st94;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto tr106;
	goto st2;
tr105:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st77;
st77:
	if ( ++p == pe )
		goto _test_eof77;
case 77:
#line 1411 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 41: goto tr108;
		case 44: goto tr109;
		case 73: goto st82;
		case 92: goto st94;
	}
	goto st2;
tr109:
#line 39 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start - 2), element_start + 1 ); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st78;
tr114:
#line 33 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("%.*s\t", (int)((long) p - (long) element_start + 1), element_start - 1 ); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st78;
tr132:
#line 46 "sql_parser.rl"
	{ 
    if((1 << field_counter) & field_mask && values_flag == 1) {
      printf("\t"); 
    }
    field_counter += 1; 
  }
#line 32 "sql_parser.rl"
	{ element_start = p + 1; }
	goto st78;
st78:
	if ( ++p == pe )
		goto _test_eof78;
case 78:
#line 1458 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st77;
		case 40: goto st76;
		case 73: goto st82;
		case 78: goto st95;
		case 92: goto st94;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st79;
	goto st2;
tr106:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st79;
st79:
	if ( ++p == pe )
		goto _test_eof79;
case 79:
#line 1477 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 41: goto tr113;
		case 44: goto tr114;
		case 46: goto st80;
		case 73: goto st82;
		case 92: goto st94;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st79;
	goto st2;
st80:
	if ( ++p == pe )
		goto _test_eof80;
case 80:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 92: goto st94;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st81;
	goto st2;
st81:
	if ( ++p == pe )
		goto _test_eof81;
case 81:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 41: goto tr113;
		case 44: goto tr114;
		case 73: goto st82;
		case 92: goto st94;
	}
	if ( 48 <= (*p) && (*p) <= 57 )
		goto st81;
	goto st2;
st82:
	if ( ++p == pe )
		goto _test_eof82;
case 82:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 78: goto st83;
		case 92: goto st94;
	}
	goto st2;
st83:
	if ( ++p == pe )
		goto _test_eof83;
case 83:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 83: goto st84;
		case 92: goto st94;
	}
	goto st2;
st84:
	if ( ++p == pe )
		goto _test_eof84;
case 84:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 69: goto st85;
		case 73: goto st82;
		case 92: goto st94;
	}
	goto st2;
st85:
	if ( ++p == pe )
		goto _test_eof85;
case 85:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 82: goto st86;
		case 92: goto st94;
	}
	goto st2;
st86:
	if ( ++p == pe )
		goto _test_eof86;
case 86:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 84: goto st87;
		case 92: goto st94;
	}
	goto st2;
st87:
	if ( ++p == pe )
		goto _test_eof87;
case 87:
	switch( (*p) ) {
		case 32: goto st88;
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 92: goto st94;
	}
	goto st2;
st88:
	if ( ++p == pe )
		goto _test_eof88;
case 88:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st89;
		case 92: goto st94;
	}
	goto st2;
st89:
	if ( ++p == pe )
		goto _test_eof89;
case 89:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 78: goto st90;
		case 92: goto st94;
	}
	goto st2;
st90:
	if ( ++p == pe )
		goto _test_eof90;
case 90:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 83: goto st84;
		case 84: goto st91;
		case 92: goto st94;
	}
	goto st2;
st91:
	if ( ++p == pe )
		goto _test_eof91;
case 91:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 79: goto st92;
		case 92: goto st94;
	}
	goto st2;
st92:
	if ( ++p == pe )
		goto _test_eof92;
case 92:
	switch( (*p) ) {
		case 32: goto st93;
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 92: goto st94;
	}
	if ( 9 <= (*p) && (*p) <= 13 )
		goto st93;
	goto st2;
st93:
	if ( ++p == pe )
		goto _test_eof93;
case 93:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 92: goto st94;
		case 96: goto st22;
	}
	goto st2;
st94:
	if ( ++p == pe )
		goto _test_eof94;
case 94:
	switch( (*p) ) {
		case 39: goto st77;
		case 40: goto st76;
		case 73: goto st82;
		case 92: goto st94;
	}
	goto st2;
tr107:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st95;
st95:
	if ( ++p == pe )
		goto _test_eof95;
case 95:
#line 1683 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 85: goto st96;
		case 92: goto st94;
	}
	goto st2;
st96:
	if ( ++p == pe )
		goto _test_eof96;
case 96:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 76: goto st97;
		case 92: goto st94;
	}
	goto st2;
st97:
	if ( ++p == pe )
		goto _test_eof97;
case 97:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 76: goto st98;
		case 92: goto st94;
	}
	goto st2;
st98:
	if ( ++p == pe )
		goto _test_eof98;
case 98:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 41: goto tr131;
		case 44: goto tr132;
		case 73: goto st82;
		case 92: goto st94;
	}
	goto st2;
st99:
	if ( ++p == pe )
		goto _test_eof99;
case 99:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 65: goto st100;
		case 73: goto st82;
		case 92: goto st94;
	}
	goto st2;
st100:
	if ( ++p == pe )
		goto _test_eof100;
case 100:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 76: goto st101;
		case 92: goto st94;
	}
	goto st2;
st101:
	if ( ++p == pe )
		goto _test_eof101;
case 101:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 85: goto st102;
		case 92: goto st94;
	}
	goto st2;
st102:
	if ( ++p == pe )
		goto _test_eof102;
case 102:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 69: goto st103;
		case 73: goto st82;
		case 92: goto st94;
	}
	goto st2;
st103:
	if ( ++p == pe )
		goto _test_eof103;
case 103:
	switch( (*p) ) {
		case 39: goto st3;
		case 40: goto st76;
		case 73: goto st82;
		case 83: goto tr137;
		case 92: goto st94;
	}
	goto st2;
tr103:
#line 57 "sql_parser.rl"
	{ values_flag = 1; }
	goto st104;
st104:
	if ( ++p == pe )
		goto _test_eof104;
case 104:
#line 1797 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 86: goto st105;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st105:
	if ( ++p == pe )
		goto _test_eof105;
case 105:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 65: goto st106;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st106:
	if ( ++p == pe )
		goto _test_eof106;
case 106:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 76: goto st107;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st107:
	if ( ++p == pe )
		goto _test_eof107;
case 107:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 85: goto st108;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st108:
	if ( ++p == pe )
		goto _test_eof108;
case 108:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 69: goto st109;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st109:
	if ( ++p == pe )
		goto _test_eof109;
case 109:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 83: goto tr143;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
tr82:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st110;
st110:
	if ( ++p == pe )
		goto _test_eof110;
case 110:
#line 1880 "sql_parser.c"
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 85: goto st111;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st111:
	if ( ++p == pe )
		goto _test_eof111;
case 111:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 76: goto st112;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st112:
	if ( ++p == pe )
		goto _test_eof112;
case 112:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 73: goto st62;
		case 76: goto st113;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
st113:
	if ( ++p == pe )
		goto _test_eof113;
case 113:
	switch( (*p) ) {
		case 39: goto st23;
		case 40: goto st56;
		case 41: goto tr147;
		case 44: goto tr148;
		case 73: goto st62;
		case 92: goto st74;
		case 96: goto tr42;
	}
	goto st22;
tr5:
#line 31 "sql_parser.rl"
	{ element_start = p + 1; field_counter = 0;}
	goto st114;
st114:
	if ( ++p == pe )
		goto _test_eof114;
case 114:
#line 1938 "sql_parser.c"
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 85: goto st115;
	}
	goto st0;
st115:
	if ( ++p == pe )
		goto _test_eof115;
case 115:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 76: goto st116;
	}
	goto st0;
st116:
	if ( ++p == pe )
		goto _test_eof116;
case 116:
	switch( (*p) ) {
		case 40: goto st1;
		case 73: goto st8;
		case 76: goto st117;
	}
	goto st0;
st117:
	if ( ++p == pe )
		goto _test_eof117;
case 117:
	switch( (*p) ) {
		case 40: goto st1;
		case 41: goto tr152;
		case 44: goto tr153;
		case 73: goto st8;
	}
	goto st0;
	}
	_test_eof0: cs = 0; goto _test_eof; 
	_test_eof1: cs = 1; goto _test_eof; 
	_test_eof2: cs = 2; goto _test_eof; 
	_test_eof3: cs = 3; goto _test_eof; 
	_test_eof4: cs = 4; goto _test_eof; 
	_test_eof5: cs = 5; goto _test_eof; 
	_test_eof6: cs = 6; goto _test_eof; 
	_test_eof7: cs = 7; goto _test_eof; 
	_test_eof8: cs = 8; goto _test_eof; 
	_test_eof9: cs = 9; goto _test_eof; 
	_test_eof10: cs = 10; goto _test_eof; 
	_test_eof11: cs = 11; goto _test_eof; 
	_test_eof12: cs = 12; goto _test_eof; 
	_test_eof13: cs = 13; goto _test_eof; 
	_test_eof14: cs = 14; goto _test_eof; 
	_test_eof15: cs = 15; goto _test_eof; 
	_test_eof16: cs = 16; goto _test_eof; 
	_test_eof17: cs = 17; goto _test_eof; 
	_test_eof18: cs = 18; goto _test_eof; 
	_test_eof19: cs = 19; goto _test_eof; 
	_test_eof20: cs = 20; goto _test_eof; 
	_test_eof21: cs = 21; goto _test_eof; 
	_test_eof22: cs = 22; goto _test_eof; 
	_test_eof23: cs = 23; goto _test_eof; 
	_test_eof24: cs = 24; goto _test_eof; 
	_test_eof25: cs = 25; goto _test_eof; 
	_test_eof26: cs = 26; goto _test_eof; 
	_test_eof27: cs = 27; goto _test_eof; 
	_test_eof28: cs = 28; goto _test_eof; 
	_test_eof29: cs = 29; goto _test_eof; 
	_test_eof30: cs = 30; goto _test_eof; 
	_test_eof31: cs = 31; goto _test_eof; 
	_test_eof32: cs = 32; goto _test_eof; 
	_test_eof33: cs = 33; goto _test_eof; 
	_test_eof34: cs = 34; goto _test_eof; 
	_test_eof35: cs = 35; goto _test_eof; 
	_test_eof36: cs = 36; goto _test_eof; 
	_test_eof37: cs = 37; goto _test_eof; 
	_test_eof38: cs = 38; goto _test_eof; 
	_test_eof39: cs = 39; goto _test_eof; 
	_test_eof40: cs = 40; goto _test_eof; 
	_test_eof41: cs = 41; goto _test_eof; 
	_test_eof42: cs = 42; goto _test_eof; 
	_test_eof43: cs = 43; goto _test_eof; 
	_test_eof44: cs = 44; goto _test_eof; 
	_test_eof45: cs = 45; goto _test_eof; 
	_test_eof46: cs = 46; goto _test_eof; 
	_test_eof47: cs = 47; goto _test_eof; 
	_test_eof48: cs = 48; goto _test_eof; 
	_test_eof49: cs = 49; goto _test_eof; 
	_test_eof50: cs = 50; goto _test_eof; 
	_test_eof51: cs = 51; goto _test_eof; 
	_test_eof52: cs = 52; goto _test_eof; 
	_test_eof53: cs = 53; goto _test_eof; 
	_test_eof54: cs = 54; goto _test_eof; 
	_test_eof55: cs = 55; goto _test_eof; 
	_test_eof56: cs = 56; goto _test_eof; 
	_test_eof57: cs = 57; goto _test_eof; 
	_test_eof58: cs = 58; goto _test_eof; 
	_test_eof59: cs = 59; goto _test_eof; 
	_test_eof60: cs = 60; goto _test_eof; 
	_test_eof61: cs = 61; goto _test_eof; 
	_test_eof62: cs = 62; goto _test_eof; 
	_test_eof63: cs = 63; goto _test_eof; 
	_test_eof64: cs = 64; goto _test_eof; 
	_test_eof65: cs = 65; goto _test_eof; 
	_test_eof66: cs = 66; goto _test_eof; 
	_test_eof67: cs = 67; goto _test_eof; 
	_test_eof68: cs = 68; goto _test_eof; 
	_test_eof69: cs = 69; goto _test_eof; 
	_test_eof70: cs = 70; goto _test_eof; 
	_test_eof71: cs = 71; goto _test_eof; 
	_test_eof72: cs = 72; goto _test_eof; 
	_test_eof73: cs = 73; goto _test_eof; 
	_test_eof74: cs = 74; goto _test_eof; 
	_test_eof75: cs = 75; goto _test_eof; 
	_test_eof76: cs = 76; goto _test_eof; 
	_test_eof77: cs = 77; goto _test_eof; 
	_test_eof78: cs = 78; goto _test_eof; 
	_test_eof79: cs = 79; goto _test_eof; 
	_test_eof80: cs = 80; goto _test_eof; 
	_test_eof81: cs = 81; goto _test_eof; 
	_test_eof82: cs = 82; goto _test_eof; 
	_test_eof83: cs = 83; goto _test_eof; 
	_test_eof84: cs = 84; goto _test_eof; 
	_test_eof85: cs = 85; goto _test_eof; 
	_test_eof86: cs = 86; goto _test_eof; 
	_test_eof87: cs = 87; goto _test_eof; 
	_test_eof88: cs = 88; goto _test_eof; 
	_test_eof89: cs = 89; goto _test_eof; 
	_test_eof90: cs = 90; goto _test_eof; 
	_test_eof91: cs = 91; goto _test_eof; 
	_test_eof92: cs = 92; goto _test_eof; 
	_test_eof93: cs = 93; goto _test_eof; 
	_test_eof94: cs = 94; goto _test_eof; 
	_test_eof95: cs = 95; goto _test_eof; 
	_test_eof96: cs = 96; goto _test_eof; 
	_test_eof97: cs = 97; goto _test_eof; 
	_test_eof98: cs = 98; goto _test_eof; 
	_test_eof99: cs = 99; goto _test_eof; 
	_test_eof100: cs = 100; goto _test_eof; 
	_test_eof101: cs = 101; goto _test_eof; 
	_test_eof102: cs = 102; goto _test_eof; 
	_test_eof103: cs = 103; goto _test_eof; 
	_test_eof104: cs = 104; goto _test_eof; 
	_test_eof105: cs = 105; goto _test_eof; 
	_test_eof106: cs = 106; goto _test_eof; 
	_test_eof107: cs = 107; goto _test_eof; 
	_test_eof108: cs = 108; goto _test_eof; 
	_test_eof109: cs = 109; goto _test_eof; 
	_test_eof110: cs = 110; goto _test_eof; 
	_test_eof111: cs = 111; goto _test_eof; 
	_test_eof112: cs = 112; goto _test_eof; 
	_test_eof113: cs = 113; goto _test_eof; 
	_test_eof114: cs = 114; goto _test_eof; 
	_test_eof115: cs = 115; goto _test_eof; 
	_test_eof116: cs = 116; goto _test_eof; 
	_test_eof117: cs = 117; goto _test_eof; 

	_test_eof: {}
	}

#line 179 "sql_parser.rl"

        memcpy(buffer, pe, remainder);
        //free(p);
      }
    }
  } else {
    printf("sql_parser wiki-{categories|pagelinks}.gz 0 1 (ids of columns to print)\nProduces TSV with the selected columns.\n");
  }
  //free(p);
  return 0;
}
