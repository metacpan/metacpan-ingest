package MetaCPAN::Archive;

use strict;
use warnings;
use v5.36;

use Archive::Any ();
use Path::Tiny   ();
use Digest::file qw( digest_file_hex );

use MetaCPAN::Logger qw( log_error );

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

__END__

=head1 NAME

MetaCPAN::Archive - Extract and inspect CPAN distribution archives

=head1 SYNOPSIS

    my $archive = MetaCPAN::Archive->new( file => '/path/to/Dist-1.0.tar.gz' );
    my $dir = $archive->extract;

=head1 DESCRIPTION

Wraps L<Archive::Any> to extract CPAN distribution archives (tar.gz, zip, etc.)
and compute file checksums. Extraction uses C</mnt/scratch_disk> when available,
falling back to the system temp directory.

=head1 METHODS

=head2 new

    my $archive = MetaCPAN::Archive->new( file => $path );

Constructs an archive object. Logs warnings if the archive is impolite
(files outside the top-level directory) or naughty (absolute paths).

=head2 extract

Extracts the archive to a temporary directory and returns the directory path.

=head2 files

Returns the list of member files in the archive.

=head2 is_impolite

Returns true if the archive escapes its top-level directory.

=head2 file_digest_md5

Returns the hex MD5 digest of the archive file itself.

=head2 file_digest_sha256

Returns the hex SHA-256 digest of the archive file itself.

=cut
