package MetaCPAN::Archive;

use strict;
use warnings;
use v5.36;

use Archive::Any ();
use Path::Tiny;
use Digest::file qw< digest_file_hex >;

use MetaCPAN::Logger qw< :log :dlog >;

sub new ( $class, %args ) {
    my $file = $args{file}
        or die "Missing file\n";

    my $archive = Archive::Any->new($file);

    log_error { $file, ' is being impolite' } if $archive->is_impolite;

    log_error { $file, ' is being naughty' } if $archive->is_naughty;

    return bless {
        file    => $file,
        archive => $archive,
    }, $class;
}

sub is_impolite ($self) {
    return $self->{archive}->is_impolite;
}

sub files ($self) {
    return $self->{archive}->files;
}

sub file_digest_md5 ($self) {
    return digest_file_hex( $self->{file}, 'MD5' );
}

sub file_digest_sha256 ($self) {
    return digest_file_hex( $self->{file}, 'SHA-256' );
}

sub extract ($self) {
    my $extract_dir = $self->_extract_dir;
    $self->{archive}->extract($extract_dir);
    $self->{extract_dir} = $extract_dir;
    return $extract_dir;
}

sub _extract_dir ($self) {
    my $scratch_disk = '/mnt/scratch_disk';
    return -d $scratch_disk
        ? Path::Tiny->tempdir('/mnt/scratch_disk/tempXXXXX')
        : Path::Tiny->tempdir;
}

1;
