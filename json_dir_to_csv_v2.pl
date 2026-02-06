#!/usr/bin/env perl
# file: json_dir_to_csv_v2.pl

use strict;
use warnings;
use utf8;

use Cwd qw(abs_path);
use Encode qw(decode encode);
use File::Basename qw(fileparse);
use File::Copy qw(move);
use File::Find qw(find);
use File::Path qw(make_path remove_tree);
use File::Spec ();
use POSIX qw(strftime);

use JSON::PP ();

# =========================
# CONFIG (edit these)
# =========================
my $INPUT_DIR  = "/pkg/vdczm/home/broadzm/geip/aep_legacy_contacthistory/imp/tmp/";
my $OUTPUT_DIR = "/pkg/vdczm/home/broadzm/geip/aep_legacy_contacthistory/imp/";

my $RECURSIVE  = 1;
my $DELIMITER  = ";";
my $OUTPUT_ENCODING = "iso-8859-15";

my $WRITE_HEADER = 1;
my $SANITIZE_FOR_SQLLDR = 1;

my $CSV_PREFIX   = "aep_contacts";
my $CSV_DATE_FMT = "%d%m%Y_%H%M";

my $LOG_DIR    = "/pkg/vdczm/home/broadzm/geip/aep_legacy_contacthistory/imp/json_converter/logs/";
my $LOG_BASENAME = "json_to_csv";
my $LOG_RETENTION_DAYS = 7;

my $DONE_DIR   = "/pkg/vdczm/home/broadzm/geip/aep_legacy_contacthistory/imp/done/";
my $DONE_MUST_BE_INSIDE_INPUT = 0;

my $ABORT_ON_INVALID_JSON = 1;
my $ABORT_ON_WRITE_ERROR  = 1;
my $ABORT_ON_MOVE_ERROR   = 1;
my $ABORT_ON_DONE_CLEANUP_ERROR = 1;

my $DRY_RUN = 0;

# log skipped records (INFO). Can be huge.
my $LOG_SKIPPED_RECORDS = 1;        # 1 => log each skipped record (INFO)
my $LOG_SKIPPED_LIMIT   = 0;        # 0 => unlimited, else max per source file

# Exit code if any skipped records exist (data quality)
my $EXIT_NONZERO_ON_SKIPS = 1;
my $EXIT_CODE_ON_SKIPS    = 2;

# =========================
# FIELD MAPPING
# =========================
my @FIELD_MAPPING = (
  { dst => "ID_",                 src => "_id" },
  { dst => "LEGACYCIPSALCUSID",   src => "_telefonicagermany.identities.legacyCipSalcusID" },
  { dst => "SUBSCRIPTIONID",      src => "_telefonicagermany.subscriptionIdentities.subscriptionID" },
  { dst => "MSISDN",              src => "_telefonicagermany.subscriptionIdentities.MSISDN" },
  { dst => "FIXEDLINENUMBER",     src => "_telefonicagermany.subscriptionIdentities.fixedLineNumber" },
  { dst => "JOURNEYVERSIONID",    src => "_telefonicagermany.journeyDetails.journeyVersionID" },
  { dst => "JOURNEYVERSIONNAME",  src => "_telefonicagermany.journeyDetails.journeyVersionName" },
  { dst => "JOURNEYNODEID",       src => "_telefonicagermany.journeyDetails.journeyNodeID" },
  { dst => "JOURNEYNODENAME",     src => "_telefonicagermany.journeyDetails.journeyNodeName" },
  { dst => "OFFERID",             src => "_telefonicagermany.offerDetails[0].offerID" },
  { dst => "OFFERNAME",           src => "_telefonicagermany.offerDetails[0].offerName" },
  { dst => "PRODUCTOFFERID",      src => "_telefonicagermany.offerDetails[0].productOfferID" },
  { dst => "CHRCONTROLGROUPFLAG",      src => "_telefonicagermany.contactHistoryReporting.chrControlGroupFlag" },
  { dst => "CHRDESCRREPORTINGVALUE01", src => "_telefonicagermany.contactHistoryReporting.chrDescrReportingValue01" },
  { dst => "CHRDESCRREPORTINGVALUE02", src => "_telefonicagermany.contactHistoryReporting.chrDescrReportingValue02" },
  { dst => "CHRREPORTINGFLAG",         src => "_telefonicagermany.contactHistoryReporting.chrReportingFlag" },
  { dst => "CHRREPORTINGVALUE01",      src => "_telefonicagermany.contactHistoryReporting.chrReportingValue01" },
  { dst => "CHRREPORTINGVALUE02",      src => "_telefonicagermany.contactHistoryReporting.chrReportingValue02" },

  { dst => "CHGATTRIBUTEDATE01",   src => "_telefonicagermany.contactHistoryGenerics.chgAttributeDate01", type => "oracle_date" },
  { dst => "CHGATTRIBUTEDATE02",   src => "_telefonicagermany.contactHistoryGenerics.chgAttributeDate02", type => "oracle_date" },
  { dst => "CHGATTRIBUTEDATE03",   src => "_telefonicagermany.contactHistoryGenerics.chgAttributeDate03", type => "oracle_date" },

  { dst => "CHGATTRIBUTEDOUBLE01", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeDouble01" },
  { dst => "CHGATTRIBUTEDOUBLE02", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeDouble02" },
  { dst => "CHGATTRIBUTEDOUBLE03", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeDouble03" },
  { dst => "CHGATTRIBUTEDOUBLE04", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeDouble04" },
  { dst => "CHGATTRIBUTEDOUBLE05", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeDouble05" },

  { dst => "CHGATTRIBUTESTRING01", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeString01" },
  { dst => "CHGATTRIBUTESTRING02", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeString02" },
  { dst => "CHGATTRIBUTESTRING03", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeString03" },
  { dst => "CHGATTRIBUTESTRING04", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeString04" },
  { dst => "CHGATTRIBUTESTRING05", src => "_telefonicagermany.contactHistoryGenerics.chgAttributeString05" },

  { dst => "CHGDESCRATTRIBUTEDATE01",   src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeDate01" },
  { dst => "CHGDESCRATTRIBUTEDATE02",   src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeDate02" },
  { dst => "CHGDESCRATTRIBUTEDATE03",   src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeDate03" },
  { dst => "CHGDESCRATTRIBUTEDOUBLE01", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeDouble01" },
  { dst => "CHGDESCRATTRIBUTEDOUBLE02", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeDouble02" },
  { dst => "CHGDESCRATTRIBUTEDOUBLE03", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeDouble03" },
  { dst => "CHGDESCRATTRIBUTEDOUBLE04", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeDouble04" },
  { dst => "CHGDESCRATTRIBUTEDOUBLE05", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeDouble05" },
  { dst => "CHGDESCRATTRIBUTESTRING01", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeString01" },
  { dst => "CHGDESCRATTRIBUTESTRING02", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeString02" },
  { dst => "CHGDESCRATTRIBUTESTRING03", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeString03" },
  { dst => "CHGDESCRATTRIBUTESTRING04", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeString04" },
  { dst => "CHGDESCRATTRIBUTESTRING05", src => "_telefonicagermany.contactHistoryGenerics.chgDescrAttributeString05" },

  { dst => "EVENTTYPE",           src => "eventType" },
  { dst => "PRODUCEDBY",          src => "producedBy" },
  { dst => "TIMESTAMP",           src => "timestamp", type => "oracle_date" },
);

die "DELIMITER must be exactly 1 character\n" unless defined($DELIMITER) && length($DELIMITER) == 1;
die "FIELD_MAPPING must not be empty\n" unless @FIELD_MAPPING;

my $in_dir   = File::Spec->rel2abs($INPUT_DIR);
my $out_dir  = File::Spec->rel2abs($OUTPUT_DIR);
my $log_dir  = File::Spec->rel2abs($LOG_DIR);
my $done_dir = defined($DONE_DIR) ? File::Spec->rel2abs($DONE_DIR) : File::Spec->catdir($in_dir, "done");

die "Input directory not found: $in_dir\n" unless -d $in_dir;
make_path($out_dir)  unless -d $out_dir;
make_path($log_dir)  unless -d $log_dir;
make_path($done_dir) unless -d $done_dir;

my $in_dir_abs   = abs_path($in_dir)   // $in_dir;
my $done_dir_abs = abs_path($done_dir) // $done_dir;

cleanup_old_logs($log_dir, $LOG_BASENAME, $LOG_RETENTION_DAYS);

my $run_ts   = strftime("%Y%m%d_%H%M%S", localtime());
my $log_file = File::Spec->catfile($log_dir, "${LOG_BASENAME}_${run_ts}.log");
open(my $log_fh, ">:encoding(UTF-8)", $log_file) or die "open($log_file): $!\n";

my $load_date = strftime($CSV_DATE_FMT, localtime());

my $json = JSON::PP->new->utf8->allow_nonref;
my $json_compact = JSON::PP->new->utf8->allow_nonref->canonical(1);

sub now_ts { strftime("%Y-%m-%d %H:%M:%S", localtime()) }

sub log_info  { my ($fh,$m)=@_; my $l="[".now_ts()."] INFO  $m\n"; print STDERR $l; print {$fh} $l; }
sub log_error { my ($fh,$m)=@_; my $l="[".now_ts()."] ERROR $m\n"; print STDERR $l; print {$fh} $l; }
sub log_warn  { my ($fh,$m)=@_; my $l="[".now_ts()."] WARN  $m\n"; print STDERR $l; print {$fh} $l; }

sub avoid_overwrite_path {
  my ($path) = @_;
  return $path unless -e $path;

  my ($vol, $dir, $file) = File::Spec->splitpath($path);
  my ($base, $unused_dir, $ext) = fileparse($file, qr/\.[^.]*/);
  $ext ||= '';

  my $suffix = strftime("%H%M%S", localtime());
  return File::Spec->catpath($vol, $dir, "${base}_${suffix}${ext}");
}

my $csv_path = File::Spec->catfile($out_dir, "${CSV_PREFIX}_${load_date}.csv");
$csv_path = avoid_overwrite_path($csv_path) if -e $csv_path;

log_info(
  $log_fh,
  "=== START " . now_ts() .
  " | in=$in_dir_abs out=$out_dir done=$done_dir_abs recursive=$RECURSIVE delim=$DELIMITER header=$WRITE_HEADER dry_run=$DRY_RUN enc=$OUTPUT_ENCODING sanitize=$SANITIZE_FOR_SQLLDR csv=$csv_path ==="
);

eval { assert_safe_done_dir($in_dir_abs, $done_dir_abs, $DONE_MUST_BE_INSIDE_INPUT); 1; } or do {
  my $err = $@ || "unknown error";
  log_error($log_fh, "ERROR done-dir safety check failed: $err");
  close $log_fh;
  die "Aborted: unsafe done dir: $err";
};

if ($DRY_RUN) {
  log_info($log_fh, "DRY-RUN: would clear done dir: $done_dir_abs");
} else {
  my $ok = eval { empty_dir($done_dir_abs); 1; };
  if (!$ok) {
    my $err = $@ || "unknown error";
    log_error($log_fh, "ERROR clearing done dir $done_dir_abs: $err");
    if ($ABORT_ON_DONE_CLEANUP_ERROR) {
      close $log_fh;
      die "Aborted: cannot clear done dir $done_dir_abs: $err";
    }
  } else {
    log_info($log_fh, "CLEARED done dir: $done_dir_abs");
  }
}

my @json_files = collect_json_files($in_dir_abs, $RECURSIVE, $done_dir_abs);
if (!@json_files) {
  log_info($log_fh, "No .json/.jsonl/.ndjson files found in: $in_dir_abs");
  log_info($log_fh, "=== END " . now_ts() . " ===");
  close $log_fh;
  exit 0;
}

my $skipped_total = 0;

if ($DRY_RUN) {
  log_info($log_fh, "DRY-RUN: would create consolidated CSV: $csv_path");
} else {
  open(my $csv_fh, ">:raw", $csv_path) or die "open($csv_path): $!\n";

  if ($WRITE_HEADER) {
    my @hdr = map { $_->{dst} } @FIELD_MAPPING;
    print {$csv_fh} join($DELIMITER, map { csv_escape($_, $DELIMITER, $OUTPUT_ENCODING) } @hdr), "\n"
      or die "write($csv_path): $!\n";
  }

  for my $src (@json_files) {
    my $raw = eval { slurp_bytes($src) };
    if ($@) { log_error($log_fh, "ERROR reading $src: $@"); next; }

    my $text = eval { decode('UTF-8', $raw, 1) };
    if ($@) { log_error($log_fh, "ERROR decoding UTF-8 $src: $@"); next; }
    $text =~ s/^\x{FEFF}//;

    my ($data, $parse_mode) = eval { parse_json_auto($text, $json) };
    if ($@) {
      my $err = $@;
      log_error($log_fh, "ERROR parsing JSON ($src): $err");
      if ($ABORT_ON_INVALID_JSON) {
        close $csv_fh;
        log_info($log_fh, "=== ABORT (invalid JSON) " . now_ts() . " ===");
        close $log_fh;
        die "Aborted due to invalid JSON in $src: $err";
      }
      next;
    }

    log_info($log_fh, "PARSE mode=$parse_mode src=$src");

    my $src_skipped = 0;

    my $write_ok = eval {
      $src_skipped = write_records_to_csv_with_filter(
        $csv_fh,
        $src,
        $data,
        $DELIMITER,
        \@FIELD_MAPPING,
        $json_compact,
        $SANITIZE_FOR_SQLLDR,
        $log_fh,
        $LOG_SKIPPED_RECORDS,
        $LOG_SKIPPED_LIMIT
      );
      1;
    };

    if (!$write_ok) {
      my $err = $@ || "unknown error";
      log_error($log_fh, "ERROR writing rows for $src => $csv_path: $err");
      if ($ABORT_ON_WRITE_ERROR) {
        close $csv_fh;
        close $log_fh;
        die "Aborted due to CSV write failure for $src: $err";
      }
      next;
    }

    $skipped_total += $src_skipped;

    log_info($log_fh, "OK  appended $src -> $csv_path");

    my $moved = eval { move_json_to_done($src, $in_dir_abs, $done_dir_abs); 1; };
    if (!$moved) {
      my $err = $@ || "unknown error";
      log_error($log_fh, "ERROR moving to done: $src: $err");
      if ($ABORT_ON_MOVE_ERROR) {
        close $csv_fh;
        close $log_fh;
        die "Aborted due to move failure for $src: $err";
      }
    }
  }

  close $csv_fh or die "close($csv_path): $!\n";
}

my $exit_code = 0;
if (!$DRY_RUN && $EXIT_NONZERO_ON_SKIPS && $skipped_total > 0) {
  log_info($log_fh, "Skips detected: skipped_total=$skipped_total => exiting with code $EXIT_CODE_ON_SKIPS");
  $exit_code = $EXIT_CODE_ON_SKIPS;
}

log_info($log_fh, "=== END " . now_ts() . " ===");
close $log_fh;
exit $exit_code;

# =========================
# helpers
# =========================

sub assert_safe_done_dir {
  my ($input_abs, $done_abs, $must_be_inside_input) = @_;

  die "done dir is empty\n"  unless defined($done_abs) && length($done_abs);
  die "input dir is empty\n" unless defined($input_abs) && length($input_abs);

  my $norm_done  = File::Spec->canonpath($done_abs);
  my $norm_input = File::Spec->canonpath($input_abs);

  die "done dir equals input dir ($norm_done)\n" if $norm_done eq $norm_input;
  die "done dir is filesystem root ($norm_done)\n" if $norm_done eq File::Spec->rootdir();
  die "done dir is drive root ($norm_done)\n" if $norm_done =~ /^[A-Za-z]:[\/\\]?\z/;
  die "done dir is relative dot path ($norm_done)\n" if $norm_done eq '.' || $norm_done eq '..';

  if ($must_be_inside_input) {
    my $input_prefix = $norm_input;
    $input_prefix .= File::Spec->catfile('') unless $input_prefix =~ /[\/\\]\z/;

    my $done_cmp = $norm_done;
    $done_cmp .= File::Spec->catfile('') unless $done_cmp =~ /[\/\\]\z/;

    die "done dir is not inside input dir\n" unless index($done_cmp, $input_prefix) == 0;
  }
  return 1;
}

sub cleanup_old_logs {
  my ($dir, $basename, $retention_days) = @_;
  my $cutoff = time() - int($retention_days * 24 * 60 * 60);

  opendir(my $dh, $dir) or die "Cannot open log dir $dir: $!\n";
  while (my $e = readdir($dh)) {
    next if $e eq '.' || $e eq '..';
    next unless $e =~ /^\Q$basename\E_\d{8}_\d{6}\.log\z/;
    my $p = File::Spec->catfile($dir, $e);
    next unless -f $p;
    my @st = stat($p);
    next unless @st;
    unlink($p) if $st[9] < $cutoff;
  }
  closedir($dh);
}

sub empty_dir {
  my ($dir) = @_;
  opendir(my $dh, $dir) or die "Cannot open dir $dir: $!\n";
  my @entries = grep { $_ ne '.' && $_ ne '..' } readdir($dh);
  closedir($dh);

  for my $e (@entries) {
    my $p = File::Spec->catfile($dir, $e);
    remove_tree($p, { error => \my $err });
    if ($err && @$err) {
      my @msgs;
      for my $item (@$err) {
        for my $k (keys %$item) {
          push @msgs, "$k: $item->{$k}";
        }
      }
      die "remove_tree failed for $p: " . join("; ", @msgs);
    }
  }
}

sub collect_json_files {
  my ($root_abs, $recursive, $done_abs) = @_;

  my @files;
  if ($recursive) {
    find(
      {
        wanted => sub {
          my $path = $File::Find::name;

          if (-d $path) {
            my $abs = abs_path($path) // $path;
            if (File::Spec->canonpath($abs) eq File::Spec->canonpath($done_abs)) {
              $File::Find::prune = 1;
            }
            return;
          }

          return unless -f $path;
          return unless $path =~ /\.(?:json|jsonl|ndjson)\z/i;
          push @files, $path;
        },
        no_chdir => 1,
      },
      $root_abs
    );
  } else {
    opendir(my $dh, $root_abs) or die "Cannot open dir $root_abs: $!\n";
    while (my $e = readdir($dh)) {
      next if $e eq '.' || $e eq '..';
      my $p = File::Spec->catfile($root_abs, $e);
      next unless -f $p;
      next unless $p =~ /\.(?:json|jsonl|ndjson)\z/i;
      push @files, $p;
    }
    closedir($dh);
  }

  return sort @files;
}

sub slurp_bytes {
  my ($path) = @_;
  open(my $fh, "<:raw", $path) or die "open($path): $!\n";
  local $/;
  my $b = <$fh>;
  close $fh;
  return $b;
}

sub move_json_to_done {
  my ($src, $input_root_abs, $done_root_abs) = @_;

  my $rel = File::Spec->abs2rel($src, $input_root_abs);
  my ($name, $dir, $ext) = fileparse($rel, qr/\.[^.]*/);

  my $dest_dir = File::Spec->catdir($done_root_abs, $dir // '');
  make_path($dest_dir) unless -d $dest_dir;

  my $dest = File::Spec->catfile($dest_dir, $name . ($ext // '.json'));
  if (-e $dest) {
    my $suffix = strftime("%Y%m%d_%H%M%S", localtime());
    $dest = File::Spec->catfile($dest_dir, "${name}_${suffix}" . ($ext // '.json'));
  }

  move($src, $dest) or die "move($src -> $dest) failed: $!";
}

# -------------------------
# AUTO PARSER: json | ndjson | sequence
# returns ($data, $mode)
# -------------------------
sub parse_json_auto {
  my ($text, $json_plain) = @_;

  $text //= '';
  $text =~ s/^\x{FEFF}//;
  $text =~ s/\x00//g;
  $text =~ s/\x1A//g;

  my $data = eval { $json_plain->decode($text) };
  return ($data, "json") if !$@;

  my $whole_err = $@;

  my @items;
  my $nd_ok = 1;
  my $saw_nonempty = 0;

  for my $line (split /\n/, $text) {
    $line =~ s/\r\z//;
    $line =~ s/^\x{FEFF}//;
    $line =~ s/\x00//g;
    $line =~ s/\x1A//g;
    $line =~ s/^\s+//;
    $line =~ s/\s+\z//;

    next if $line eq '';
    $saw_nonempty = 1;

    my $obj = eval { $json_plain->decode($line) };
    if ($@) { $nd_ok = 0; last; }
    push @items, $obj;
  }

  return (\@items, "ndjson") if $nd_ok && $saw_nonempty && @items;

  my $incr = JSON::PP->new->utf8->allow_nonref;
  my @seq;

  my $ok = eval {
    my $obj = $incr->incr_parse($text);
    push @seq, $obj if defined $obj;

    while (1) {
      my $next = $incr->incr_parse;
      last unless defined $next;
      push @seq, $next;
    }

    my $rest = $incr->incr_text // '';
    $rest =~ s/\x00//g;
    $rest =~ s/\x1A//g;
    $rest =~ s/\r//g;
    $rest =~ s/^\s+//;
    $rest =~ s/\s+\z//;

    die "Trailing non-whitespace after JSON sequence: '$rest'" if length($rest);
    1;
  };

  return (\@seq, "sequence") if $ok && @seq;

  die "Invalid JSON/NDJSON/sequence: $whole_err";
}

sub normalize_records {
  my ($data) = @_;
  my $r = ref($data) || '';
  return @$data if $r eq 'ARRAY';
  return ($data) if $r eq 'HASH';
  return ({ value => $data });
}

sub tokenize_json_path {
  my ($path) = @_;
  return () unless defined $path && length $path;

  my @tokens;
  my $s = $path;

  while (length $s) {
    if ($s =~ s/^([^.[]+)//) {
      push @tokens, { type => 'key', val => $1 };
    }
    while ($s =~ s/^\[(\d+)\]//) {
      push @tokens, { type => 'idx', val => int($1) };
    }
    $s =~ s/^\.//;
    last if $s eq '';
  }

  return @tokens;
}

sub get_by_path {
  my ($node, $path) = @_;
  return undef unless defined $path && length $path;

  my @tokens = tokenize_json_path($path);
  my $cur = $node;

  for my $t (@tokens) {
    return undef unless defined $cur;

    if ($t->{type} eq 'key') {
      return undef unless ref($cur) eq 'HASH' && exists $cur->{ $t->{val} };
      $cur = $cur->{ $t->{val} };
      next;
    }

    if ($t->{type} eq 'idx') {
      return undef unless ref($cur) eq 'ARRAY';
      my $i = $t->{val};
      return undef if $i < 0 || $i >= @$cur;
      $cur = $cur->[$i];
      next;
    }

    return undef;
  }

  return $cur;
}

sub scalar_value_for_log {
  my ($v, $json_compact_ref, $sanitize) = @_;
  return '' unless defined $v;

  my $r = ref($v) || '';
  my $s;

  if ($r eq 'JSON::PP::Boolean') {
    $s = $v ? 'true' : 'false';
  } elsif ($r) {
    $s = $json_compact_ref->encode($v);
  } else {
    $s = "$v";
  }

  if ($sanitize) {
    $s =~ s/\x00//g;
    $s =~ s/\r\n|\r|\n/ /g;
  }

  return $s;
}

sub value_is_present {
  my ($v, $json_compact_ref, $sanitize) = @_;
  return 0 unless defined $v;

  my $s = scalar_value_for_log($v, $json_compact_ref, $sanitize);
  $s =~ s/^\s+//;
  $s =~ s/\s+\z//;

  return length($s) > 0 ? 1 : 0;
}

sub write_records_to_csv_with_filter {
  my ($csv_fh, $src_file, $data, $delimiter, $mapping, $json_compact_ref, $sanitize, $log_fh, $log_skipped, $skipped_limit) = @_;

  my @records = normalize_records($data);

  my $req_path_04 = "_telefonicagermany.contactHistoryGenerics.chgAttributeString04";
  my $req_path_05 = "_telefonicagermany.contactHistoryGenerics.chgAttributeString05";
  my $id_path     = "_id";

  my $written = 0;
  my $skipped = 0;
  my $skipped_logged = 0;

  for my $rec (@records) {
    my $v04 = get_by_path($rec, $req_path_04);
    my $v05 = get_by_path($rec, $req_path_05);

    my $ok04 = value_is_present($v04, $json_compact_ref, $sanitize);
    my $ok05 = value_is_present($v05, $json_compact_ref, $sanitize);

    unless ($ok04 && $ok05) {
      $skipped++;

      if ($log_skipped && (!$skipped_limit || $skipped_logged < $skipped_limit)) {
        my $rid = get_by_path($rec, $id_path);
        my $rid_s = defined($rid) ? scalar_value_for_log($rid, $json_compact_ref, $sanitize) : '';
        my $v04_s  = scalar_value_for_log($v04, $json_compact_ref, $sanitize);
        my $v05_s  = scalar_value_for_log($v05, $json_compact_ref, $sanitize);

        my $reason = (!$ok04 && !$ok05) ? "missing_both"
                   : (!$ok04)           ? "missing_chgAttributeString04"
                   :                      "missing_chgAttributeString05";

        log_info(
          $log_fh,
          "SKIP record src=$src_file _id='$rid_s' reason=$reason CHGATTRIBUTESTRING04='$v04_s' CHGATTRIBUTESTRING05='$v05_s'"
        );
        $skipped_logged++;
      }

      next;
    }

    my @row = map {
      my $v = get_by_path($rec, $_->{src});
      normalize_mapped_value($v, $_->{type}, $json_compact_ref, $delimiter, $sanitize, $log_fh, $src_file, $_->{dst});
    } @$mapping;

    print {$csv_fh} join($delimiter, map { csv_escape($_, $delimiter, $OUTPUT_ENCODING) } @row), "\n"
      or die "write(csv): $!\n";

    $written++;
  }

  my $limit_note = ($log_skipped && $skipped_limit) ? " (logged up to limit=$skipped_limit, logged=$skipped_logged)" : "";
  log_info(
    $log_fh,
    "FILTER applied for $src_file: written=$written skipped=$skipped$limit_note (requires CHGATTRIBUTESTRING04+05 non-empty)"
  );

  return $skipped;
}

sub normalize_mapped_value {
  my ($v, $type, $json_compact_ref, $delimiter, $sanitize, $log_fh, $src_file, $dst_name) = @_;
  return '' unless defined $v;

  my $r = ref($v) || '';
  my $s;

  if ($r eq 'JSON::PP::Boolean') {
    $s = $v ? 'true' : 'false';
  } elsif ($r) {
    $s = $json_compact_ref->encode($v);
  } else {
    $s = "$v";
  }

  if (defined($type) && $type eq 'oracle_date') {
    my $conv = iso_to_oracle_ddmmyyyy_hh24miss($s);
    if (!defined $conv) {
      log_warn($log_fh, "Unparseable date for $dst_name in $src_file -> empty (value='$s')");
      $s = '';
    } else {
      $s = $conv;
    }
  }

  if ($sanitize) {
    $s =~ s/\x00//g;
    $s =~ s/\r\n|\r|\n/ /g;
  }

  return $s;
}

sub iso_to_oracle_ddmmyyyy_hh24miss {
  my ($s) = @_;
  return '' if !defined($s) || $s eq '';

  return $s if $s =~ /^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}\z/;

  if ($s =~ /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+\-]\d{2}:?\d{2})?\z/) {
    my ($Y, $m, $d, $H, $M, $S) = ($1, $2, $3, $4, $5, $6);
    return sprintf("%02d.%02d.%04d %02d:%02d:%02d", $d, $m, $Y, $H, $M, $S);
  }

  return undef;
}

sub csv_escape {
  my ($v, $delimiter, $out_enc) = @_;
  $v = '' unless defined $v;
  $v = "$v";

  my $needs_quotes = 0;
  $needs_quotes = 1 if index($v, $delimiter) >= 0;
  $needs_quotes = 1 if $v =~ /["\r\n]/;

  if ($needs_quotes) {
    $v =~ s/"/""/g;
    $v = qq{"$v"};
  }

  return encode($out_enc, $v, 1);
}
