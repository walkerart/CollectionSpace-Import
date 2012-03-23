

=head1 NAME Table::Art - provides interface to object & related tables

=head1 SYNOPSIS

use Table::Art;

$art = Table::Art->new(primary_key => $pk);

$bool   = $art->exists();

($filepath,$filename,$imgH,$imgW,$imgDesc,$imgCaption,$imgCopyright,
    $imgTerm) = $art->get_preferred_img();

($citation,$content) = $art->get_preferred_text();

($ownerID,$ownerName,$cmsID,$amicoID,$creatorFwd,$creatorRev,$createDt,
 $createStartYr,$createEndYr,$createLoc,$materialTechnique,$measure,
 $inscript,$workState,$edition,$printer,$publisher,$pysDesc, 
 $accessionNum,$oldAcessionNum,$callNum,$credits,$cPermission,
 $copyright,$cLinkID,$daLinkId,$technology,$dept, $title)
   = $art->get_details();

(@otherTitles,@classifications,@sytles,@subjects) = $art->get_contexts();

($url,$anchor) = $art->get_link_by_id( $linkId );

@results = $art->assoc_links();

@results = $art->assoc_texts();

@results = $art->assoc_archives();

@results = $art->assoc_ed_units();

@results = $art->assoc_agents();

@results = $art->assoc_events();

@results = $art->assoc_objects();


=head1 DESCRIPTION

Table::Art - provides interface to object & related tables

=cut

package Table::Art;
use Table::Base;
use Table::Ed_Unit;
use Table::Archive;
use Table::Media;
use Table::Text;
use Table::Words;
use CNRutils;
use XML::DOM;
use AOC::Work;
@ISA = ("Table::Base");
$VERSION = 0.01;
use strict;

sub new {
  my ($class, %arg) = @_;
  my $self = bless {
      _art_id  => $arg{primary_key}
    }, ref($class) || $class;
  $$self{_dbh} = $self->_get_dbh();
  return $self;
}

sub DESTROY {
  my $self = shift;
  $$self{_dbh}->disconnect();
}

sub exists {
  #
  # returns 1 if we know about a name with the supplied info, 0 otherwise
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "exists():Missing OBJECT primary key!" unless $id;
  my $query = "SELECT COUNT(*) FROM object WHERE id = $id AND deleted = 'f'";
  my ($count) = $self->_get_one_row($query ,$Table::Base::MUST_RETURN_ONE_ROW);
  ($count > 0) ? return 1 : return 0;
}

sub set_id {
  my ($self, $id) = @_;
  die "must call set_id with a primary key" unless ($id);
  $$self{_art_id} = $id;
}

sub get_preferred_text {
  #
  # Return preferred text
  #  (citation, content)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "get_preferred_text(): Missing OBJECT primary key!" unless $id;
  my $query = "SELECT t.citation, t.content " .
        "FROM object_text ot, texts t " .
        "WHERE ((ot.preferred_text IS TRUE) AND (ot.object_id = $id) " .
        "AND (t.id = ot.text_id))";
  return $self->_get_one_row($query);
}

sub get_details {
  #
  # Return object information
  #  (ownerID, ownerName, cmsID, amicoID, creatorFwd, creatorRev, createDt,
  #   createStartYr, createEndYr, createLoc, materialTechnique, measure,
  #   inscript, workState, edition, printer, publisher, pysDesc, 
  #   accessionNum, oldAcessionNum, callNum, credits, cPermission,
  #   copyright, cLinkID, daLinkId, technology, dept, title)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "get_details(): Missing OBJECT primary key!" unless $id;
  my $query = "select owner_id from object where id = $id";
  my ($owner_id) = $self->_get_one_row($query);
  my $owner_name;
  if ($owner_id) {
    $query = "select owner_name from owner where id = $owner_id";
    ($owner_name) = $self->_get_one_row($query);
  }
      
  $query = "select cms_id, 
      amico_id, creator_text_forward, 
      creator_text_inverted, creation_date_text, 
      creation_start_year, creation_end_year, 
      creation_place, materials_techniques_text, 
      measurement_text, inscriptions_marks, 
      work_state, edition, 
      printer, publisher, 
      physical_description, accession_number, 
      old_accession_number, call_no, 
      credit_line, copyright_permission, 
      copyright, copyright_link_id, 
      da_link_id, technology_used, 
      department, title.title
    FROM object, title
    WHERE object.id = $id 
      AND title.object_id = object.id
      AND title.title_type = 'P'";
  return($owner_id, $owner_name, $self->_get_one_row($query));
}

sub get_contexts {
  #
  # Return object contexts
  #  (otherTitles, classifications, sytles, subjects)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "get_contexts(): Missing OBJECT primary key!" unless $id;

  # Get other titles...
  my $query = "SELECT id, title_type, title " .
	"FROM title WHERE object_id = $id AND title_type <> 'P'";
  my $otherTitles = $self->_get_all_rows($query);

  # Get Classifications...
  $query = "SELECT distinct c.id, c.term 
    FROM classification c, object_classification " .
    "o WHERE ((o.object_id = $id) AND (c.id = o.classification_id))";
  my $classifications = $self->_get_all_rows($query);

  # Get Styles/Periods... 
  $query = "SELECT s.id, s.term FROM style s, object_style o " .
    "WHERE ((o.object_id = $id) AND (s.id = o.style_id))";
  my $styles = $self->_get_all_rows($query);

  # Get Subjects...
  $query = "SELECT s.id, s.term FROM subject s, object_subject o " .
    "WHERE ((o.object_id = $id) AND (s.id = o.subject_id))";
  my $subjects = $self->_get_all_rows($query);

  return $otherTitles,$classifications,$styles,$subjects;
}

sub get_link_by_id {
  #
  # Return link detail for a given link id
  #	  (url, anchor)
  my $self = shift;
  my $linkId = shift;
  die "get_link(): Missing LINK primary key arg!" unless $linkId;
  my $query = "SELECT url, anchor FROM link where id = $linkId";
  return $self->_get_one_row($query);
}

# object_link
sub assoc_links {
  #
  # Return related links (results)
  #
  #  Results returned is an array (rows)
  #  of array refs (columns: linkURL, linkAnchor)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "get_links(): Missing OBJECT primary key!" unless $id;
  my $query = "SELECT l.url, l.anchor " .
        "FROM object_link ol, link l " .
        "WHERE ((ol.object_id = $id) AND (l.id = ol.link_id))";
  return $self->_get_all_rows($query);
}

sub g9_links {
  my $self = shift;
  my $id = $$self{_art_id};
  die "g9_links(): Missing primary key!" unless $id;
  my $query = "SELECT url, anchor
        FROM gallery_9, link 
        WHERE gallery_9.table_type='object'
          and gallery_9.link_id = link.id
          and gallery_9.target_id = $id";
  return $self->_get_all_rows($query);
}

# object_text
sub assoc_texts {
  #
  # Return related non-preferred text (results)
  #
  #  Results returned is an array (rows) of array refs
  # (columns: textid, ownerId, linkId, type, citation, src, txtdate, 
  #  title, content, dept, copyright)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "get_texts(): Missing OBJECT primary key!" unless $id;
  my $query = "SELECT t.id, t.owner_id, t.link_id, t.text_type, t.citation, " .
        "t.source, t.text_date, t.text_title, t.content, " .
        "t.creator_department, t.copyright " .
        "FROM object_text ot, texts t " .
        "WHERE ((ot.object_id = $id) AND (ot.preferred_text IS NOT TRUE) " .
        "AND (t.id = ot.text_id))";
  return $self->_get_all_rows($query);
}

# archive_object
sub assoc_archives {
  #
  # Return related archives (results)
  #
  #  Results returned is an array (rows) of array refs
  # (columns: archiveId, ??)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "assoc_archives(): Missing OBJECT primary key!" unless $id;
  my $query = "SELECT archive_id FROM archive_object WHERE object_id = $id";
  return $self->_get_all_rows($query);
}

# ed_unit_object
sub assoc_ed_units {
  #
  # Return related ed_units (results)
  #
  #  Results returned is an array (rows) of array refs
  # (columns: eduId, ??)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "assoc_ed_units(): Missing OBJECT primary key!" unless $id;
  my $query = "SELECT ed_unit_id FROM ed_unit_object WHERE object_id = $id";
  return $self->_get_all_rows($query);
}

# object_agent
sub assoc_agents {
  #
  # Return related agents (results)
  #
  #  Results returned is an array (rows) of array refs
  # (columns: agentId, lname, pfx, fname, sfx)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "assoc_agents(): Missing OBJECT primary key!" unless $id;
  my $query = 
      "SELECT n.agent_id, n.index_name, n.prefix, n.first_name, n.suffix " .
      "FROM object_agent u INNER JOIN names n ON (u.agent_id = n.agent_id) " .
      "WHERE u.object_id = $id AND n.preferred_name IS TRUE " . 
      "ORDER BY n.index_name";
  return $self->_get_all_rows($query);
}

# object_event
sub assoc_events {
  #
  # Return related events (results)
  #
  #  Results returned is an array (rows) of array refs
  # (columns: evtId, ??)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "assoc_events(): Missing OBJECT primary key!" unless $id;
  my $query = "SELECT event_id FROM object_event WHERE object_id = $id";
  return $self->_get_all_rows($query);
}

# object_media
sub assoc_media {
  #
  # Return related media (results)
  #
  #  Results returned is an array (rows) of array refs
  # (columns: mediaId, ??)
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "assoc_media(): Missing OBJECT primary key!" unless $id;
  my $query = "SELECT media_id FROM object_media WHERE object_id = $id";
  return $self->_get_all_rows($query);
}

sub assoc_objects {
  #
  # Return related objects (results)
  #
  #  Results returned is an array (rows) of array refs
  # (columns: objId, prefTitle, ??)
  #
  # Note: this is the old pre-spec version
  #
  my $self = shift;
  my $id = $$self{_art_id};
  die "assoc_objects(): Missing OBJECT primary key!" unless $id;
  my $query = "SELECT o.related_object_id, " .
	"(SELECT title FROM title" .
	" WHERE object_id = o.related_object_id AND title_type = 'P') AS title " .
  	"FROM object_object o, object obj WHERE o.object_id = $id and obj.id = o.related_object_id and obj.deleted = 'f'";
  	warn $query;
  return $self->_get_all_rows($query);
}

sub get_related_objects {
  #
  # Return summary info for objects related to this object either
  #   via the object_object table, or that are also by the same
  #   artist.  Return a list of results with id, preferred title, 
  #   creator_text_forward, creation_date_text,
  #   and stuff for 'j' image.  Items should be ranked by having 1)
  #   images, 2) texts.  Duplicates are taken care of. If a max
  #   is passed, return only that many related objs; otherwise, return
  #   them all.
  #
  # xxx sorting currently ensures only that media / text rich
  #     records get bumped; otherwise it's random.
  #
  my ($self, $max) = @_;
  my $id = $$self{_art_id};
  my (%object_ids, @artist_ids, @object_ids);
  die "I need to know my primary key." unless $id;
  my $query = "select oo.related_object_id from object_object oo, object o where
    oo.object_id = $id AND o.id = oo.related_object_id and o.deleted='f'";
  my ($arrayref) = $self->_get_all_rows($query);
  foreach my $row (@$arrayref) {
    $object_ids{$$row[0]} = 1;
  }
  $query = "select oo.object_id from object_object oo, object o where oo.related_object_id
    = $id AND o.id = oo.object_id and o.deleted='f'";
  ($arrayref) = $self->_get_all_rows($query);
  foreach my $row (@$arrayref) {
    $object_ids{$$row[0]} = 1;
  }
  $query = "select agent_id from object_agent where object_id = $id
    and object_agent_type = 'A'";
  ($arrayref) = $self->_get_all_rows($query);
  foreach my $row (@$arrayref) {
    push(@artist_ids, $$row[0]);
  }
  # NS - 1/19/05
  # this is broken if there are no artist_ids!
  return 0 if (! @artist_ids > 0);
  $query = "select oa.object_id from object_agent oa, object o where oa.object_agent_type 
    ='A' and oa.agent_id in (" . join (', ', @artist_ids) . ") and o.id = oa.object_id and o.deleted='f' limit 150";
  ($arrayref) = $self->_get_all_rows($query);
  foreach my $row (@$arrayref) {
    $object_ids{$$row[0]} = 1;
  }
  #
  # don't want to return the id we start with...
  #
  delete $object_ids{$id};
  #
  # phew, we should have all related ids now.  Time to rank them
  #   (via having an image and having text).
  #
  @object_ids = $self->sort_by_richness(\%object_ids, $max);
  return 0 if (! @object_ids > 0);
  return $self->get_summary_info(@object_ids);
}

sub sort_by_richness {
  my ($self, $aref, $max) = @_;
  my %object_ids = %$aref;
  my @object_ids;
  foreach my $object_id (keys %object_ids) {
    my $query = "select count(*) from object_media, object
      where object_id = $object_id
      and object_id = id
      and copyright_permission = 't'";
    my ($count) = $self->_get_one_row($query);
    $object_ids{$object_id} += 20 if ($count);
  }
  foreach my $object_id (keys %object_ids) {
    my $query = "select count(*) from object_text where object_id = $object_id";
    my ($count) = $self->_get_one_row($query);
    $object_ids{$object_id} += 10 if ($count);
  }
  @object_ids = sort { $object_ids{$b} <=> $object_ids{$a} } keys %object_ids;
  if ($max and ($max < @object_ids)) {
    @object_ids = @object_ids[0..--$max];
  }
  return @object_ids;
}

sub get_summary_info {
  my ($self, @object_ids) = @_;
  my $query = "select object.id, title, creator_text_forward, 
      creation_date_text, file_name_path, file_name, height, 
      width, description, caption, media.copyright, description_term, 
      copyright_permission
    from title, object
      LEFT JOIN object_media ON (object_media.object_id = object.id
        and preferred_image IS TRUE)
      LEFT JOIN media ON (media.id = object_media.media_id
        and media_type = 'Digital Image')
      LEFT JOIN renditions ON (renditions.media_id = media.id and
       rendition_code = 'j')
    where
      title.object_id = object.id
      and title_type = 'P'
      and object.id in (" . join(', ', @object_ids) . ")";
  my $results = $self->_get_all_rows($query);
  return $results;
}

sub get_related_artists {
  #
  # Return ids and namestring (forward) of artists & technicians related to 
  # the object.
  # Limit to max (if sent).
  #
  my ($self, $max) = @_;
  my $id = $$self{_art_id};
  my @results;
  my (%object_ids, @artist_ids, @object_ids);
  die "I need to know my primary key." unless $id;
  my $query = "select agent_id from object_agent where object_agent_type
    in ('A', 'T') and object_id = $id";
  my $agentsref = $self->_get_all_rows($query);
  foreach my $agent_row_ref (@$agentsref) {
    $query = "select first_name, index_name from names where preferred_name
      = 't' and agent_id = $$agent_row_ref[0]";
    my ($firstname, $lastname) = $self->_get_one_row($query);
    push(@results, { id => $$agent_row_ref[0], first_name => $firstname, 
      last_name => $lastname });
  }
  if ($max and ($max < @results)) {
    @results = @results[0..--$max];
  }
  return @results;
}

sub get_related_concepts {
  #
  # return ids, concept types, and terms.
  # Limit to max (if set).
  #
  my ($self, $max) = @_;
  my $id = $$self{_art_id};
  die "I need to know my primary key." unless $id;
  my (@results);
  my $query = "select distinct style_id, term from object_style,
    style where style.id = style_id 
    and object_id = $id order by term asc";
  my ($arrayref) = $self->_get_all_rows($query);
  foreach my $row (@$arrayref) {
    push(@results, { id => $$row[0], type => 'style', term => $$row[1]});
  }
  $query = "select distinct subject_id, term from object_subject,
    subject where subject.id = subject_id 
    and object_id = $id order by term asc";
  ($arrayref) = $self->_get_all_rows($query);
  foreach my $row (@$arrayref) {
    push(@results, { id => $$row[0], type => 'subject', term => $$row[1]});
  }
  if ($max and ($max < @results)) {
    @results = @results[0..--$max];
  }
  return @results;
}

sub get_related_events {
  #
  # Return ids, event_types, and titles.
  # Limit to max (if set).
  #
  my ($self, $max) = @_;
  my $id = $$self{_art_id};
  die "I need to know my primary key." unless $id;
  my (@results);
  my $query = "select event_id, event_type, title from
    object_event, event
    where event_id = event.id and object_id = $id
    order by title asc";
  my ($arrayref) = $self->_get_all_rows($query);
  foreach my $row (@$arrayref) {
    push(@results, { id => $$row[0], type => $$row[1], title => $$row[2]});
  }
  if ($max and ($max < @results)) {
    @results = @results[0..--$max];
  }
  return @results;
}

sub get_related_resources {
  #
  # Potentially returns texts, archive_units, educational_resources.
  # Priority given to archives & ed_units with online content.
  # Limit to max (if set).
  # 
  my ($self, $max) = @_;
  my $id = $$self{_art_id};
  die "I need to know my primary key." unless $id;
  my (@results, @texts, @archives, @ed_units, %archive_ids);
  my $query = "select distinct id, text_type, citation from object_text, texts
    where texts.id = object_text.text_id and object_id = $id
    and preferred_text = 'f' order by text_type";
  my ($arrayref) = $self->_get_all_rows($query);
  foreach my $row (@$arrayref) {
    push(@texts, { id => $$row[0], text_type => $$row[1], 
      citation => $$row[2], type => 'text' });
  }

  #
  # xxx maybe it is better to get all archive_ids, weight them,
  #     and then get type and title?
  # In any event, still need weighting
  #
  my $archive = Table::Archive->new();
  $query = "select distinct archive_unit.id, archive_type, 
    title from archive_object, archive_unit
    where archive_id = archive_unit.id and object_id = $id";
  ($arrayref) = $self->_get_all_rows($query);
  foreach my $row (@$arrayref) {
    my $type = $$row[1];
    $archive->set_id($$row[0]);
    $type = "ONLINE $type" if ($archive->media_exists());
    push(@archives, { id => $$row[0], archive_type => $type, 
      title => $$row[2], type => 'archive'});
    $archive_ids{$$row[0]} = 1;
  }
  my (@artists) = $self->get_related_artists($max);
  my @artist_ids;
  foreach my $artist_ref (@artists) {
    push(@artist_ids, $$artist_ref{id});
  }
  if ($artist_ids[0]) {
    $query = "select distinct archive_unit.id, archive_type, title
      from archive_agent, archive_unit
      where agent_id in (" . join (', ', @artist_ids) . ")
        and archive_id = archive_unit.id";
    ($arrayref) = $self->_get_all_rows($query);
    foreach my $row (@$arrayref) {
      my $type = $$row[1];
      $archive->set_id($$row[0]);
      $type = "ONLINE $type" if ($archive->media_exists());
      push(@archives, 
        { id => $$row[0], archive_type => $type, title => $$row[2],
          type => 'archive' }
        ) unless $archive_ids{$$row[0]};;
    }
  }
  #
  # end archive section that is questionable
  #

  $query = "select distinct id, type, unit_title, availability from ed_unit,
    ed_unit_object where object_id = $id and ed_unit_id = id
    order by availability, unit_title";
  ($arrayref) = $self->_get_all_rows($query);
  my $ed_unit = Table::Ed_Unit->new();
  foreach my $row (@$arrayref) {
    push(@ed_units, { id => $$row[0], ed_type => $$row[1], 
      unit_title => $$row[2], availability => $$row[3], type => 'ed_unit'});
  }
  #
  # Now we have populated texts, archives, and ed_units.  Merge somehow,
  # do something about the max(), and return them. Or return things
  # on their own?
  #
  foreach my $row (@texts)    { push(@results, $row); }
  foreach my $row (@archives) { push(@results, $row); }
  foreach my $row (@ed_units) { push(@results, $row); }
  if ($max and ($max < @results)) {
    @results = @results[0..--$max];
  }
  return @results;

}

sub get_permitted_imaged {
  #
  # Returns pks of all art objects that have images and are viewable according
  #   to copyright.
  my $self = shift;
  my $query = "select distinct object.id
    from object, object_media
    where object.id = object_media.object_id
    and object.copyright_permission='t'";
  my $arrayref = $self->_get_all_rows($query);
  my @results;
  foreach my $row (@$arrayref) {
    push(@results, $$row[0]);
  }
  return @results;
}

sub media_exists {
  my $self = shift;
  my $id = $$self{_art_id} or die "need to know my id";
  my $query = "select count(*) from object_media where object_id = $id";
  my ($count) = $self->_get_one_row($query, $Table::Base::MUST_RETURN_ONE_ROW);
  return 1 if $count;
  $query = "select da_link_id from object where id = $id";
  ($count) = $self->_get_one_row($query, $Table::Base::MUST_RETURN_ONE_ROW);
  return 1 if $count;
  return 0;
}

sub text_exists {
  my $self = shift;
  my $id = $$self{_art_id} or die "need to know my id";
  my $query = "select count(*) from object_text
    where object_id = $id";
  my ($count) = $self->_get_one_row($query, $Table::Base::MUST_RETURN_ONE_ROW);
  $count > 0 ? return 1: return 0;
}

sub access_exists {
  my $self = shift;
  my $id = $$self{_art_id} or die "need to know my id";
  my $query = "select count(*) from access_point_vote
    where object_id = $id";
  my ($count) = $self->_get_one_row($query, $Table::Base::MUST_RETURN_ONE_ROW);
  $count > 0 ? return 1: return 0;
}

sub comment_exists {
  my $self = shift;
  my $id = $$self{_art_id} or die "need to know my id";
  my $query = "select count(*) from comment
    where item_type = 'object'
    and item_id = $id";
  my ($count) = $self->_get_one_row($query, $Table::Base::MUST_RETURN_ONE_ROW);
  $count > 0 ? return 1: return 0;
}

sub highlight_exists {
  my $self = shift;
  my $id = $$self{_art_id} or die "need to know my id";
  my $query = "select count(*) from highlights
    where table_type = 'object'
    and id = $id";
  my ($count) = $self->_get_one_row($query, $Table::Base::MUST_RETURN_ONE_ROW);
  $count > 0 ? return 1: return 0;
}

sub get_media_ids {
  #
  # Returns a list containing the media_ids from object_media.  If
  # there is a preferred image, it will come first.
  #
  my ($self, $rendition_code) = @_;
  my $id = $$self{_art_id} or die "need to know my id";
  die "need a rendition_code" unless $rendition_code;
  my $query = "select object_media.media_id from object_media, renditions
    where object_id = $id and rendition_code = '$rendition_code'
    and object_media.media_id = renditions.media_id
    order by preferred_image DESC";
  my ($arrayref) = $self->_get_all_rows($query);
  my @results;
  foreach my $row (@$arrayref) {
    #warn "GOT MEDIA: $$row[0]";
    push(@results, $$row[0]);
  }
  return @results;
}

sub get_media_records {
  #
  # xxx what happens when there are multiple renditions of a streaming
  # media item???
  #
  my ($self) = @_;
  my $id = $$self{_art_id} or die "need to know my id";
  my $query = "SELECT r.ref_file_path, r.ref_file_name, r.file_type,
    r.length, m.set, m.description_term, m.caption, m.copyright, 
    m.description_term, media_type, m.id
    FROM object_media om, media m, renditions r
    WHERE om.object_id = $id
    AND m.id = om.media_id
    AND r.media_id = m.id
    and (media_type = 'audio' or media_type = 'video')
    ORDER BY m.id, media_type, file_type, m.set, description_term";
  return $self->_get_all_rows($query);
}

sub get_cdwalite_xml {
  my ($a, $id) = @_;
  
  my $dom = new XML::DOM::Document;
  my $root = $dom->appendChild($dom->createElement('metadata'));
  
  my $query = "select o.id, 
      o.amico_id, o.creator_text_forward, 
      o.creator_text_inverted, o.creation_date_text, 
      o.creation_start_year, o.creation_end_year, 
      o.creation_place, o.materials_techniques_text, 
      o.measurement_text, o.inscriptions_marks, 
      o.work_state, o.edition, 
      o.printer, o.publisher, 
      o.physical_description, o.accession_number, 
      o.old_accession_number, o.call_no, 
      o.credit_line, o.copyright_permission, 
      o.copyright, o.copyright_link_id, 
      o.da_link_id, o.technology_used, 
      o.department, t.title, o.update_date
    FROM object o, title t
    WHERE o.id = $id 
      AND t.object_id = o.id
      AND t.title_type = 'P'";

  my ($cnrID,$amicoID,$creatorFwd,$creatorRev,$createDt,
          $createStartYr,$createEndYr,$createLoc,$materialTechnique,$measure,
          $inscript,$workState,$edition,$printer,$publisher,$pysDesc, 
          $accessionNum,$oldAcessionNum,$callNum,$credits,$cPermission,
          $copyright,$cLinkID,$daLinkId,$technology,$dept, $title, $update_date) = $a->_get_one_row($query);
  return 0 if (!$cnrID); # this means there's no title!  WTF??

  # before we start, we need to check a few things:
  # is this a work from gallery 9?
  # -- i.e. does it have an entry in the gallery_9 table?
  my $g9url = "";
  if ($daLinkId) {
      $query = "select l.url, l.anchor FROM link l WHERE l.id = $daLinkId";
      my ($g9ref) = $a->_get_all_rows($query);
  
      foreach (@$g9ref) {
        my ($g9link, $g9anchor) = @$_;
        warn "MORE THAN ONE Gallery 9 LINK!! $g9link" if ($g9url);
        $g9url = $g9link;
      }
  }
  warn "Gallery 9 LINK!! $g9url" if ($g9url);

  my $cd = $root->appendChild($dom->createElement('cdwalite:cdwalite'));
  $cd->setAttribute('xmlns:cdwalite','http://www.getty.edu/CDWA/CDWALite/');
  $cd->setAttribute('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance');
  $cd->setAttribute('xsi:schemaLocation','http://www.getty.edu/CDWA/CDWALite/ http://www.getty.edu/CDWA/CDWALite/CDWALite-xsd-public-v1-1.xsd');

  ################# object type query:
  $query = "select c.term, c.scheme from object_classification oc, classification c
      where oc.object_id = $cnrID and oc.classification_id = c.id";
  my ($tmpref) = $a->_get_all_rows($query);
  # wrapper element
  my $typexml = $dom->createElement('cdwalite:objectWorkTypeWrap');
  foreach (@$tmpref) {
    my ($term, $scheme) = @$_;
    my $type = $dom->createElement('cdwalite:objectWorkType');
    $type->setAttribute('cdwalite:termsource',$scheme);
    $type->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($term))));
    $typexml->appendChild($type);
  }
  $cd->appendChild($typexml);

  ################ object title query
  $query = "select t.title, t.title_type from title t where t.object_id = $cnrID";
  ($tmpref) = $a->_get_all_rows($query);
  # wrapper element
  my $wrapxml = $dom->createElement('cdwalite:titleWrap');
  foreach (@$tmpref) {
    my ($thetitle, $theterm) = @$_;
    my $set = $dom->createElement('cdwalite:titleSet');
    my $ttl = $dom->createElement('cdwalite:title');
    $ttl->setAttribute('cdwalite:pref',$theterm eq 'P' ? 'preferred' : 'alternate');
    $ttl->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($thetitle))));
    $set->appendChild($ttl);
    $wrapxml->appendChild($set);
  }
  $cd->appendChild($wrapxml);



  ################# creator stuff
  # sets first, collect into for displayable element later
  my $displayCreatorTxt = "";
  my $close_paren = 0;

  # wrapper element
  $wrapxml = $dom->createElement('cdwalite:indexingCreatorWrap');
  $query = "SELECT a.id, a.gender, a.nationality, a.ethnicity, a.date_text, " .
    "a.start_date, a.end_date, a.birth_place, a.death_place, a.active_date_place, " .
    "oa.role " .
    "FROM agent a, object_agent oa WHERE a.id = oa.agent_id and oa.object_id=$cnrID";
  ($tmpref) = $a->_get_all_rows($query);

  # we may need to split a bunch of stupid strings up into individual creators:
  my @useNames = split(',',$creatorFwd);
  my $do_multiname_hack = 0;
  my $name_index = 0;
  my $orig_size = @$tmpref;
  # if there are more, we need to... be smart.
  #if (@useNames > 1 && @$tmpref == 1) {
  # NS 7/9/08 - we need a reversed name, so this can't work.  Sorry, all you hidden collaborators...
  if (0 && @useNames > $orig_size) {
    # ok, shit: one agent, but the displaycreator indicates several.  We need to fake this.
    # what we're gonna do: set a flag, and push a bunch of fakes into $tmpref so we can keep the loop going and fill it
    $do_multiname_hack = 1;
    for(my $i=1;$i<@useNames;$i++) { # less one since it's got one to start
      push(@$tmpref,@$tmpref[0]);
    }
  }
  my $loopcount=0;
  foreach (@$tmpref) {
    my $multinamehack = ($do_multiname_hack && ($loopcount >= ($orig_size-1)));
    my ($aid, $gender, $nationality, $ethnicity, $date_text, $start_date, $end_date, $birth_place,
        $death_place, $active_date_place, $role) = @$_;
    # what if these are null??  skip?
    
    # this needs DB cleanup, but for now since we know ...
    next if ($aid == 5442); # this is the empty author that everyone has.  Skip it.
    

    my $indexingCreatorSetXml = $dom->createElement('cdwalite:indexingCreatorSet');

    # get all names
    my $subq = "SELECT first_name, index_name, prefix, suffix, preferred_name " .
        "FROM names WHERE agent_id = $aid";
    my ($tmpref2) = $a->_get_all_rows($subq);
    
    my $gotname = 0;
    foreach (@$tmpref2) {
      my ($fname, $lname, $prefix, $suffix, $preferred) = @$_;
      # skip if no name??  why is this in our db??
      #next if (!$fname and !$lname);
      # NS 3/21/08 - actually, let's try to use the fwd and revCreator,
      # since we've got them...
      my $fwdName = $creatorFwd;
      my $revName = $creatorRev;
      if ($lname) {
        # ah, got something, let's use that instead:
        $fwdName = $fname . ($fname ? " " : "") . $lname;
        $revName = $fname ? ("$lname, $fname") : $lname;
      }
      $displayCreatorTxt .= ($displayCreatorTxt ? ", " : "") . $fwdName;
      $gotname = 1;
      #my $usename = $fwdName;
      my $usename = $revName;
      # are we in a special looping case?
      if ($multinamehack) {
        $usename = $useNames[$name_index];
        $name_index++;
      }
      # NS 7/8/08 - no, leave them in.  This is a reverse name now.  Hope this is ok...
      ## if the usename has commas, blow it up and use the first.  WTF.
      #my @splode = split(',',$usename);
      #if (@splode > 1) { $usename = $splode[0]; }
      my $nameCreatorSetXml = $dom->createElement('cdwalite:nameCreatorSet');
      $usename =~ s/^\s+//;
      $usename =~ s/\s+$//;
      #$usename .= "hack:$multinamehack";
      my $nameCreatorXml = $dom->createElement('cdwalite:nameCreator');
      $nameCreatorXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($usename))));
      $nameCreatorSetXml->appendChild($nameCreatorXml);
      $indexingCreatorSetXml->appendChild($nameCreatorSetXml);
    }
    #next if (!$gotname); # skip this whole creator
    $loopcount++;
    # nationality
    if ($nationality && !$multinamehack) {
      $close_paren = 1;
      $displayCreatorTxt .= " ($nationality";
      my $nationalityCreatorXml = $dom->createElement('cdwalite:nationalityCreator');
      $nationalityCreatorXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($nationality))));
      $indexingCreatorSetXml->appendChild($nationalityCreatorXml);
    }

    # vital dates
    if ($date_text && !$multinamehack) {
      if ($nationality) { $displayCreatorTxt .= ", $date_text"; }
      else { $displayCreatorTxt .= " ($date_text"; }
      $close_paren = 1;
      my $vitalDatesCreatorXml = $dom->createElement('cdwalite:vitalDatesCreator');
      $vitalDatesCreatorXml->setAttribute('cdwalite:birthdate',$start_date) if ($start_date);
      $vitalDatesCreatorXml->setAttribute('cdwalite:deathdate',$end_date) if ($end_date);
      $vitalDatesCreatorXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($date_text))));
      $indexingCreatorSetXml->appendChild($vitalDatesCreatorXml);
    }

    # gender
    if ($gender && !$multinamehack) {
      my $genderCreatorXml = $dom->createElement('cdwalite:genderCreator');
      my $gen = 'unknown';
      $gen = 'male' if ($gender eq 'M');
      $gen = 'female' if ($gender eq 'F');
      $genderCreatorXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($gen))));
      $indexingCreatorSetXml->appendChild($genderCreatorXml);
    }
    
    # role
    if ($role && !$multinamehack) {
      my $roleCreatorXml = $dom->createElement('cdwalite:roleCreator');
      $roleCreatorXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($role))));
      $indexingCreatorSetXml->appendChild($roleCreatorXml);
    }
    if ($multinamehack) {
      my $roleCreatorXml = $dom->createElement('cdwalite:roleCreator');
      $roleCreatorXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8('artist'))));
      $indexingCreatorSetXml->appendChild($roleCreatorXml);
    }

    # qualifier not in our db
    # extent not in our db

    $wrapxml->appendChild($indexingCreatorSetXml);

  } # creators

  # displayCreator goes first
  $displayCreatorTxt .= ")" if ($close_paren);
  #warn $displayCreatorTxt;
  # ...actually, looks like things depend on using $creatorFwd...
  #warn $creatorFwd;
  $displayCreatorTxt = $creatorFwd;
  my $displayCreatorXml = $dom->createElement('cdwalite:displayCreator');
  $displayCreatorXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8(($displayCreatorTxt)))));
  $cd->appendChild($displayCreatorXml);

  #  then add the indexingCreatorSet
  $cd->appendChild($wrapxml);


  ################# measurements
  # display version:
  my $displayMeasurements = $dom->createElement('cdwalite:displayMeasurements');
  $displayMeasurements->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($measure))));
  $cd->appendChild($displayMeasurements);

  # now the set
  # wrapper element
  $wrapxml = $dom->createElement('cdwalite:indexingMeasurementsWrap');
  $query = "SELECT m.measurement_type, m.unit, m.value " .
    "FROM measurement m WHERE m.object_id=$cnrID order by m.id desc";
  ($tmpref) = $a->_get_all_rows($query);
  my $indexingMeasurementsSetXml = $dom->createElement('cdwalite:indexingMeasurementsSet');
  my %seen_type;
  foreach (@$tmpref) {
    my ($mtype, $munit, $mvalue) = @$_;
    my $measurementsSetXml = $dom->createElement('cdwalite:measurementsSet');
    my $measure_type = "height";
    $measure_type = "width" if (uc($mtype) eq 'W');
    $measure_type = "depth" if (uc($mtype) eq 'D');
    next if ($seen_type{$measure_type});
    $seen_type{$measure_type} = 1;
    $measurementsSetXml->setAttribute('cdwalite:type',$measure_type);
    $measurementsSetXml->setAttribute('cdwalite:value',$mvalue);
    my $measure_unit = "in"; # WAC doesn't do anything else?
    $measurementsSetXml->setAttribute('cdwalite:unit',$measure_unit);
    $indexingMeasurementsSetXml->appendChild($measurementsSetXml);
  } # measurements
  $wrapxml->appendChild($indexingMeasurementsSetXml);
  $cd->appendChild($wrapxml);


  ################# Materials / tech
  # display version:
  if ($materialTechnique) {
    my $displayMaterialsTech = $dom->createElement('cdwalite:displayMaterialsTech');
    $displayMaterialsTech->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($materialTechnique))));
    $cd->appendChild($displayMaterialsTech);
  }
  # WAC doesn't have any more detail in the db...

# <cdwalite:displayStateEditionWrap>
#  <cdwalite:displayState> 1st of 3 states (Robison (1986)) </cdwalite:displayState>
#  <cdwalite:sourceStateEdition> Andrew Robison, Early Architectural Fantasies: A
#    Catalogue Raisonné of the Piranesi Etchings. Washington, DC: National Gallery of Art,
#    1986. </cdwalite:sourceStateEdition>
# </cdwalite:displayStateEditionWrap>


  ################# Materials / tech
  # display version:
  if ($workState || $edition) {
    $wrapxml = $dom->createElement('cdwalite:displayStateEditionWrap');
    if ($workState) {
      my $tmpXml = $dom->createElement('cdwalite:displayState');
      $tmpXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($workState))));
      $wrapxml->appendChild($tmpXml);
    }
    if ($edition) {
      my $tmpXml = $dom->createElement('cdwalite:displayEdition');
      $tmpXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($edition))));
      $wrapxml->appendChild($tmpXml);
    }
    $cd->appendChild($wrapxml);
  }


  ################# style
  # wrapper element
  $wrapxml = $dom->createElement('cdwalite:styleWrap');
  $query = "SELECT s.term FROM style s, object_style os " .
    "WHERE s.id = os.style_id and os.object_id=$cnrID";
  ($tmpref) = $a->_get_all_rows($query);
  my $hasStyle = 0;
  foreach (@$tmpref) {
    my ($sterm) = @$_;
    $hasStyle = 1;
    my $styleXml = $dom->createElement('cdwalite:style');
    $styleXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($sterm))));
    $wrapxml->appendChild($styleXml);
  } # style
  $cd->appendChild($wrapxml) if ($hasStyle);


  # no culture in WAC


  ################# creation dates
  # display version:
  if ($createDt) {
    my $displayCreationDate = $dom->createElement('cdwalite:displayCreationDate');
    $displayCreationDate->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($createDt))));
    $cd->appendChild($displayCreationDate);
  }
  if ($createStartYr || $createEndYr) {
    # wrapper element
    $wrapxml = $dom->createElement('cdwalite:indexingDatesWrap');
    my $indexingDatesSetXml = $dom->createElement('cdwalite:indexingDatesSet');
    if ($createStartYr) {
      my $earliestDate = $dom->createElement('cdwalite:earliestDate');
      $earliestDate->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($createStartYr))));
      $indexingDatesSetXml->appendChild($earliestDate);
    }if ($createEndYr) {
      my $latestDate = $dom->createElement('cdwalite:latestDate');
      $latestDate->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($createEndYr))));
      $indexingDatesSetXml->appendChild($latestDate);
    }
    $wrapxml->appendChild($indexingDatesSetXml);
  } # creation
  $cd->appendChild($wrapxml);
  
  
  # no location in WAC - or should we assume it's us?
  # ################## location - it's at the WAC!

  # wrapper element
  $wrapxml = $dom->createElement('cdwalite:locationWrap');
  my $locationSetXml = $dom->createElement('cdwalite:locationSet');
  my $locationNameXml = $dom->createElement('cdwalite:locationName');
  $locationNameXml->setAttribute("cdwalite:type","currentRepository");
  $locationNameXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8("Walker Art Center (Minneapolis, MN, USA)"))));
  my $workIDXml = $dom->createElement('cdwalite:workID');
  $workIDXml->setAttribute("cdwalite:type","accession");
  $workIDXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($accessionNum))));
  $locationSetXml->appendChild($locationNameXml);
  $locationSetXml->appendChild($workIDXml);
  $wrapxml->appendChild($locationSetXml);
  
  $locationSetXml = $dom->createElement('cdwalite:locationSet');
  $locationNameXml = $dom->createElement('cdwalite:locationName');
  $locationNameXml->setAttribute("cdwalite:type","currentCredit");
  if ($credits) {
    $credits = '; '.$credits;
  }
  $credits = 'Collection Walker Art Center'.$credits;
  $locationNameXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($credits))));
  $locationSetXml->appendChild($locationNameXml);
  $wrapxml->appendChild($locationSetXml);
  
  # gallery / on view
  $query = "SELECT l.description FROM location l, object_location ol ".
    "WHERE l.id = ol.location_id and ol.object_id=$cnrID";
  ($tmpref) = $a->_get_all_rows($query);
  my $location = 'IGotNothing';
  foreach (@$tmpref) {
    ($location) = @$_;
    warn "$location\n";
  }
  if ($location ne 'IGotNothing')
  {
    $locationSetXml = $dom->createElement('cdwalite:locationSet');
    $locationNameXml = $dom->createElement('cdwalite:locationName');
    $locationNameXml->setAttribute("cdwalite:type","galleryLocation");
    my $use_loc = 'On view at the Walker Art Center';
    if ($location) {
      $use_loc .= ', '.$location;
    }
    $locationNameXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($use_loc))));
    $locationSetXml->appendChild($locationNameXml);
    $wrapxml->appendChild($locationSetXml);
  } elsif ($g9url)
  {
    $locationSetXml = $dom->createElement('cdwalite:locationSet');
    $locationNameXml = $dom->createElement('cdwalite:locationName');
    $locationNameXml->setAttribute("cdwalite:type","galleryLocation");
    my $use_loc = $g9url;
    $locationNameXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($use_loc))));
    $locationSetXml->appendChild($locationNameXml);
    $wrapxml->appendChild($locationSetXml);
  }
  
  
  $cd->appendChild($wrapxml);
  
  
  ################# subject
  # wrapper element
  $wrapxml = $dom->createElement('cdwalite:indexingSubjectWrap');
  $query = "SELECT s.term, s.scheme FROM subject s, object_subject os " .
    "WHERE s.id = os.subject_id and os.object_id=$cnrID";
  ($tmpref) = $a->_get_all_rows($query);
  my $hasSubject = 0;
  foreach (@$tmpref) {
    my ($sterm, $sscheme) = @$_;
    $hasSubject = 1;
    my $indexingSubjectSetXml = $dom->createElement('cdwalite:indexingSubjectSet');
    my $subjectTermXml = $dom->createElement('cdwalite:subjectTerm');
    $subjectTermXml->setAttribute("cdwalite:termsource",$sscheme);
    $subjectTermXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($sterm))));
    $indexingSubjectSetXml->appendChild($subjectTermXml);
    $wrapxml->appendChild($indexingSubjectSetXml);
  } # subject
  my $words = new Table::Words;
  my $ref = $words->get_words_by_ranking($cnrID,3);
  my @theWordArr = @$ref;
  foreach my $row (@theWordArr) {
    my $wword = $$row{word};
    $hasSubject = 1;
    my $indexingSubjectSetXml = $dom->createElement('cdwalite:indexingSubjectSet');
    my $subjectTermXml = $dom->createElement('cdwalite:subjectTerm');
    $subjectTermXml->setAttribute("cdwalite:termsource",'WAC_words');
    $subjectTermXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($wword))));
    $indexingSubjectSetXml->appendChild($subjectTermXml);
    $wrapxml->appendChild($indexingSubjectSetXml);
  }
  $cd->appendChild($wrapxml) if ($hasSubject);
  
  
  ################## classification
  $query = "select c.term, c.scheme from object_classification oc, classification c
      where oc.object_id = $cnrID and oc.classification_id = c.id";
  ($tmpref) = $a->_get_all_rows($query);
  # wrapper element
  $wrapxml = $dom->createElement('cdwalite:classificationWrap');
  foreach (@$tmpref) {
    my ($term, $scheme) = @$_;
    my $classification = $dom->createElement('cdwalite:classification');
    $classification->setAttribute('cdwalite:termsource',$scheme);
    $classification->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($term))));
    $wrapxml->appendChild($classification);
  }
  $cd->appendChild($wrapxml);
  
  
  ################## description
  if ($pysDesc) {
    # wrapper element
    $wrapxml = $dom->createElement('cdwalite:descriptiveNoteWrap');
    my $descNoteSetXml = $dom->createElement('cdwalite:descriptiveNoteSet');
    my $descriptiveNoteXml = $dom->createElement('cdwalite:descriptiveNote');
    $descriptiveNoteXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($pysDesc))));
    $descNoteSetXml->appendChild($descriptiveNoteXml);
    $wrapxml->appendChild($descNoteSetXml);
    $cd->appendChild($wrapxml);
  }
  
  
  ################## inscription
  if ($inscript) {
    # wrapper element
    $wrapxml = $dom->createElement('cdwalite:inscriptionsWrap');
    my $inscriptionsXml = $dom->createElement('cdwalite:inscriptions');
    $inscriptionsXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($inscript))));
    $wrapxml->appendChild($inscriptionsXml);
    $cd->appendChild($wrapxml);
  }
  
  ################## related works
  $query = "select o.id, t.title, o.creator_text_forward, o.creation_date_text FROM object o, object_object oo, title t WHERE oo.object_id=$id and o.id=oo.related_object_id and t.object_id=$id and o.deleted='f'";
  ($tmpref) = $a->_get_all_rows($query);
  # wrapper element
  $wrapxml = $dom->createElement('cdwalite:relatedWorksWrap');
  my $hasRelatedWorks = 0;
  foreach (@$tmpref) {
    my ($oid, $otitle, $ocreator, $ocreatedate) = @$_;
    $hasRelatedWorks = 1;
    my $set = $dom->createElement('cdwalite:relatedWorkSet');
    my $linkRelatedWork = $dom->createElement('cdwalite:linkRelatedWork');
    $linkRelatedWork->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8("http://collections.walkerart.org/item/object/".$oid))));
    $set->appendChild($linkRelatedWork);
    my $labelRelatedWork = $dom->createElement('cdwalite:labelRelatedWork');
    $labelRelatedWork->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8("oai:walkerart.org/object/".$oid))));
    $set->appendChild($labelRelatedWork);
    $wrapxml->appendChild($set);
  }

  my $compare_title = $title;
  $compare_title =~ s/'/''/g;
  $query = "select o.id, t.title, o.creator_text_forward, o.creation_date_text from object o join object_agent oa2 on (oa2.object_id=$id AND oa2.agent_id<>5442) join object_agent oa on (oa.object_id = o.id and oa.agent_id = oa2.agent_id) join title t on (o.id = t.object_id) where o.id<>$id
  AND upper(t.title) like upper('%$compare_title%')
  AND (
  upper(t.title) like upper('%study%')
  OR upper(t.title) like upper('%detail%')
  OR upper(t.title) like upper('%view%')
  );";
  #warn $query;
  ($tmpref) = $a->_get_all_rows($query);
  # wrapper element
  foreach (@$tmpref) {
    my ($oid, $otitle, $ocreator, $ocreatedate) = @$_;
    warn "FOUND AUTOMATIC RELATION! $cnrID : $title and $oid : $otitle";
    $hasRelatedWorks = 1;
    my $set = $dom->createElement('cdwalite:relatedWorkSet');
    my $linkRelatedWork = $dom->createElement('cdwalite:linkRelatedWork');
    $linkRelatedWork->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8("http://collections.walkerart.org/item/object/".$oid))));
    $set->appendChild($linkRelatedWork);
    my $labelRelatedWork = $dom->createElement('cdwalite:labelRelatedWork');
    $labelRelatedWork->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8("oai:walkerart.org/object/".$oid))));
    $set->appendChild($labelRelatedWork);
    $wrapxml->appendChild($set);
  }
  $cd->appendChild($wrapxml) if ($hasRelatedWorks);
  
  
  ################## rights
  # <cdwalite:rightsWork>© J. Paul Getty Museum</cdwalite:rightsWork>
  if ($copyright) {
    my $inscriptionsXml = $dom->createElement('cdwalite:rightsWork');
    $inscriptionsXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($copyright))));
    $cd->appendChild($inscriptionsXml);
  }
  
  
  ################## record info
  $wrapxml = $dom->createElement('cdwalite:recordWrap');
  
  my $recordID = $dom->createElement('cdwalite:recordID');
  $recordID->setAttribute('cdwalite:type','object');
  $recordID->appendChild($dom->createTextNode($id));
  $wrapxml->appendChild($recordID);
  
  my $recordType = $dom->createElement('cdwalite:recordType');
  $recordType->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8("item"))));
  $wrapxml->appendChild($recordType);
  
  my $recordSource = $dom->createElement('cdwalite:recordSource');
  $recordSource->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8("Walker Art Center (Minneapolis, MN, USA)"))));
  $wrapxml->appendChild($recordSource);
  
  my $wrap2 = $dom->createElement('cdwalite:recordInfoWrap');
  my $recordInfoID = $dom->createElement('cdwalite:recordInfoID');
  $recordInfoID->appendChild($dom->createTextNode("oai:walkerart.org:object/$id"));
  $wrap2->appendChild($recordInfoID);
  
  my $recordInfoLink = $dom->createElement('cdwalite:recordInfoLink');
  $recordInfoLink->appendChild($dom->createTextNode("http://collections.walkerart.org/item/object/".$id));
  $wrap2->appendChild($recordInfoLink);
  
  $wrapxml->appendChild($wrap2);
  $cd->appendChild($wrapxml);
  
  
  ################## resources
  # this includes media, text, etc, but NOT other works.
#   <cdwalite:resourceWrap>
#    <cdwalite:resourceSet>
#        <cdwalite:resourceID>2344</cdwalite:resourceID>
#        <cdwalite:resourceType> black-and-white slide </cdwalite:resourceType>
#        <cdwalite:resourceRelationshipType> historical image
#          </cdwalite:resourceRelationshipType>
#        <cdwalite:rightsResource> Janson, H.W., History of Art. 3rd edition. New York:
#          Harry N. Abrams, Inc., 1986, plate 54 </cdwalite:rightsResource>
#        <cdwalite:resourceViewDescription> frontal view, from below
#          </cdwalite:resourceViewDescription>
#        <cdwalite:resourceViewDate earliestdate="1957" latestdate="1962"> before
#          1962</cdwalite:resourceViewDate>
#    </cdwalite:resourceSet>

  # wrapper element
  $wrapxml = $dom->createElement('cdwalite:resourceWrap');
  my $hasResources = 0;
  
  if ($cPermission) {
    # media first:
    $query = "SELECT m.id, r.file_name_path, r.file_name, m.description_term,
      m.caption, m.media_type, m.copyright
      FROM media m, renditions r, object_media om
      WHERE m.id = r.media_id
        AND m.id = om.media_id
        AND r.rendition_code = 'e'
        AND om.object_id = $id
        ORDER BY om.preferred_image desc, m.id";
    ($tmpref) = $a->_get_all_rows($query);
    
    foreach (@$tmpref) {
      my ($mid, $path, $fn, $mterm, $mcaption, $mtype, $mcopyright) = @$_;
      $hasResources = 1;
      my $set = $dom->createElement('cdwalite:resourceSet');
      my $linkResource = $dom->createElement('cdwalite:linkResource');
      #http://www.walkerart.org/walker_images/e_images/01/wac_786e.jpg
      my $root = "http://images.artsmia.org:16080/";
      my $oldpath = $path;
      $path =~ s/walker_images\/e_images/wac\/orig/;
      if ($path eq $oldpath) {
        # no change, it's not an e_image - probably G9
        $root = "http://www.walkerart.org/";
      } else {
        $fn =~ s/e\.jpg/.tif/;
      }
      $linkResource->appendChild($dom->createTextNode($root.$path."/".$fn));
      $set->appendChild($linkResource);
      my $m = new Table::Media;
      $m->set_id($mid);
      my $resourceID = $dom->createElement('cdwalite:resourceID');
      $resourceID->appendChild($dom->createTextNode($m->oai_id));
      $set->appendChild($resourceID);
      my $resourceType = $dom->createElement('cdwalite:resourceType');
      $resourceType->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($mtype))));
      $set->appendChild($resourceType);
      my $rightsResource = $dom->createElement('cdwalite:rightsResource');
      $rightsResource->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($mcopyright))));
      $set->appendChild($rightsResource);
      my $resourceViewDescription = $dom->createElement('cdwalite:resourceViewDescription');
      $resourceViewDescription->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($mcaption))));
      $set->appendChild($resourceViewDescription);
      my $resourceViewType = $dom->createElement('cdwalite:resourceViewType');
      $resourceViewType->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($mterm))));
      $set->appendChild($resourceViewType);
      $wrapxml->appendChild($set);
    }
    $cd->appendChild($wrapxml) if ($hasResources);
  }
  
  # text next:
  $query = $query = "SELECT t.id, t.citation, t.text_title, t.copyright, t.text_type " .
        "FROM object_text ot, texts t " .
        "WHERE (ot.object_id = $id) " .
        "AND (t.id = ot.text_id)
        ORDER BY ot.preferred_text desc, t.id";
  ($tmpref) = $a->_get_all_rows($query);
  
  # wrapper element
  $wrapxml = $dom->createElement('cdwalite:resourceWrap');
  $hasResources = 0;
  foreach (@$tmpref) {
    my ($tid, $tcite, $ttitle, $tcopyright, $ttype) = @$_;
    $hasResources = 1;
    my $set = $dom->createElement('cdwalite:resourceSet');
    my $linkResource = $dom->createElement('cdwalite:linkResource');
    $linkResource->appendChild($dom->createTextNode("http://collections.walkerart.org/item/text/".$tid));
    $set->appendChild($linkResource);
    my $t = new Table::Text;
    $t->set_id($tid);
    my $resourceID = $dom->createElement('cdwalite:resourceID');
    $resourceID->appendChild($dom->createTextNode($t->oai_id));
    $set->appendChild($resourceID);
    my $resourceType = $dom->createElement('cdwalite:resourceType');
    $resourceType->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8("text"))));
    $set->appendChild($resourceType);
    my $rightsResource = $dom->createElement('cdwalite:rightsResource');
    $rightsResource->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($tcopyright))));
    $set->appendChild($rightsResource);
    my $resourceViewDescription = $dom->createElement('cdwalite:resourceViewDescription');
    $resourceViewDescription->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($ttitle))));
    $set->appendChild($resourceViewDescription);
    my $resourceViewType = $dom->createElement('cdwalite:resourceViewType');
    $resourceViewType->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($ttype))));
    $set->appendChild($resourceViewType);
    $wrapxml->appendChild($set);
  }
  $cd->appendChild($wrapxml) if ($hasResources);
  
#   # AOC
#   my $aocWork = new AOC::Work;
#   my @aocWorks = $aocWork->get_by_cnr_id($id);  # wrapper element
#   $wrapxml = $dom->createElement('cdwalite:resourceWrap');
#   $hasResources = 0;
#   foreach (@aocWorks) {
#     $hasResources = 1;
#     my $set = $dom->createElement('cdwalite:resourceSet');
#     my $linkResource = $dom->createElement('cdwalite:linkResource');
#     $linkResource->appendChild($dom->createTextNode("http://newmedia.walkerart.org/aoc/index.wac?term=".$$_{_id}));
#     $set->appendChild($linkResource);
#     my $resourceID = $dom->createElement('cdwalite:resourceID');
#     $resourceID->appendChild($dom->createTextNode($$_{_id}));
#     $set->appendChild($resourceID);
#     my $resourceType = $dom->createElement('cdwalite:resourceType');
#     $resourceType->appendChild($dom->createTextNode("sound"));
#     $set->appendChild($resourceType);
#     my $rightsResource = $dom->createElement('cdwalite:rightsResource');
#     $rightsResource->appendChild($dom->createTextNode("Copyright Walker Art Center"));
#     $set->appendChild($rightsResource);
#     my $resourceViewDescription = $dom->createElement('cdwalite:resourceViewDescription');
#     $resourceViewDescription->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($_->get_title_short_name))));
#     $set->appendChild($resourceViewDescription);
#     my $resourceViewType = $dom->createElement('cdwalite:resourceViewType');
#     $resourceViewType->appendChild($dom->createTextNode("Audio Commentary"));
#     $set->appendChild($resourceViewType);
#     $wrapxml->appendChild($set);
#   }
#   $cd->appendChild($wrapxml) if ($hasResources);
  
  return $root;
}

# needs to support DC as well.
sub get_dc_xml {
  my ($a, $id, $qdc, $rss) = @_;
  
  $a->set_id($id);
  my $root = $rss;
  my $dom = $rss;
  if (!$dom) {
    $dom = new XML::DOM::Document;
    $root = $dom->appendChild($dom->createElement('metadata'));
  } else {
    $root = $rss->createElement('item');
  }
  
  # if rss, put that first
  if ($rss) { $a->get_rss_xml($id,$dom,1,$root); }
  
  my $query = "select o.id, 
      o.amico_id, o.creator_text_forward, 
      o.creator_text_inverted, o.creation_date_text, 
      o.creation_start_year, o.creation_end_year, 
      o.creation_place, o.materials_techniques_text, 
      o.measurement_text, o.inscriptions_marks, 
      o.work_state, o.edition, 
      o.printer, o.publisher, 
      o.physical_description, o.accession_number, 
      o.old_accession_number, o.call_no, 
      o.credit_line, o.copyright_permission, 
      o.copyright, o.copyright_link_id, 
      o.da_link_id, o.technology_used, 
      o.department, t.title, o.update_date
    FROM object o, title t
    WHERE o.id = $id 
      AND t.object_id = o.id
      AND t.title_type = 'P'";

  my ($cnrID,$amicoID,$creatorFwd,$creatorRev,$createDt,
          $createStartYr,$createEndYr,$createLoc,$materialTechnique,$measure,
          $inscript,$workState,$edition,$printer,$publisher,$pysDesc, 
          $accessionNum,$oldAcessionNum,$callNum,$credits,$cPermission,
          $copyright,$cLinkID,$daLinkId,$technology,$dept, $title, $update_date) = $a->_get_one_row($query);
  return 0 if (!$cnrID); # this means there's no title!  WTF??
          
  # before we start, we need to check a few things:
  # is this a work from gallery 9?
  # -- i.e. does it have an entry in the gallery_9 table?
  my $g9url = "";
  if ($daLinkId) {
      $query = "select l.url, l.anchor FROM link l WHERE l.id = $daLinkId";
      my ($g9ref) = $a->_get_all_rows($query);
  
      foreach (@$g9ref) {
        my ($g9link, $g9anchor) = @$_;
        warn "MORE THAN ONE Gallery 9 LINK!! $g9link" if ($g9url);
        $g9url = $g9link;
      }
  }
#  $query = "select l.url, l.anchor FROM link l WHERE l.id = $daLinkId";
#  my ($g9ref) = $a->_get_all_rows($query);
#  my $g9url = "";
#  
#  foreach (@$g9ref) {
#    my ($g9link, $g9anchor) = @$_;
#    warn "MORE THAN ONE Gallery 9 LINK!! $g9link" if ($g9url);
#    $g9url = $g9link;
#  }

  my $dc = $root;
  if (!$rss) {
    if (!$qdc) {
      $dc = $root->appendChild($dom->createElement('oai_dc:dc'));
      $dc->setAttribute('xmlns:oai_dc','http://www.openarchives.org/OAI/2.0/oai_dc/');
      $dc->setAttribute('xmlns:dc','http://purl.org/dc/elements/1.1/');
      $dc->setAttribute('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance');
      $dc->setAttribute('xsi:schemaLocation','http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd');
    } else {
      $dc = $root->appendChild($dom->createElement('qdc:qualifieddc'));
      $dc->setAttribute('xmlns:qdc','http://purl.org/dc/terms/');
      $dc->setAttribute('xmlns:dc','http://purl.org/dc/elements/1.1/');
      $dc->setAttribute('xmlns:dcterms','http://purl.org/dc/terms/');
      $dc->setAttribute('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance');
      $dc->setAttribute('xsi:schemaLocation','http://purl.org/dc/terms/ http://dublincore.org/schemas/xmls/qdc/2003/04/02/qualifieddc.xsd');
    }
  }
  
  my $xtitle = $dom->createElement("dc:title");
  $xtitle->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($title))));
  $dc->appendChild($xtitle);
  
  my $creator = $dom->createElement("dc:creator");
  $creator->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($creatorFwd))));
  $dc->appendChild($creator);
  
################## subject
  $query = "select c.term, c.scheme from object_classification oc, classification c
      where oc.object_id = $cnrID and oc.classification_id = c.id";
  my ($tmpref) = $a->_get_all_rows($query);
  foreach (@$tmpref) {
    my ($term, $scheme) = @$_;
    my $classification = $dom->createElement('dc:subject');
    $classification->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($term))));
    $dc->appendChild($classification);
  }
  
  ################## location
  my $loc = $a->location_name();
  if ($loc ne '0') {
    $pysDesc = "On view at the Walker Art Center".($loc?", $loc":'')."<br/><br/>".$pysDesc;
  }

  ################## description
  if ($pysDesc) {
    # wrapper element
    my $descriptiveNoteXml = $dom->createElement('dc:description');
    $descriptiveNoteXml->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($pysDesc))));
    $dc->appendChild($descriptiveNoteXml);
  }

################# creation dates
  if ($createStartYr) {
    my $displayCreationDate = $dom->createElement('dc:date');
    $displayCreationDate->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($createStartYr))));
    $dc->appendChild($displayCreationDate);
  }
#   if ($createEndYr && $createEndYr != $createStartYr) {
#     my $displayCreationDate = $dom->createElement('dc:date');
#     $displayCreationDate->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($createEndYr))));
#     $dc->appendChild($displayCreationDate);
#   }
  
  my $rights = $dom->createElement("dc:rights");
  $rights->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($copyright))));
  $dc->appendChild($rights);

  ################# type
  $query = "select c.term, c.scheme from object_classification oc, classification c
      where oc.object_id = $cnrID and oc.classification_id = c.id";
  ($tmpref) = $a->_get_all_rows($query);
  # wrapper element
  foreach (@$tmpref) {
    my ($term, $scheme) = @$_;
    my $typexml = $dom->createElement('dc:type');
    $typexml->appendChild($dom->createTextNode($term));
    $dc->appendChild($typexml);
  }
  
  my $identifier = $dom->createElement("dc:identifier");
  $identifier->appendChild($dom->createTextNode($a->link_id));
  $dc->appendChild($identifier);
  
  # get related objects?

  return $root;
}

sub get_rss_xml {
  my ($a, $id, $dom, $nomediaok,$into,$mediarss) = @_;
  
  $a->set_id($id);
  
  my $item = $into;
  if (!$into) { $item = $dom->createElement('item'); }
  
  my $query = "select o.id, 
      o.amico_id, o.creator_text_forward, 
      o.creator_text_inverted, o.creation_date_text, 
      o.creation_start_year, o.creation_end_year, 
      o.creation_place, o.materials_techniques_text, 
      o.measurement_text, o.inscriptions_marks, 
      o.work_state, o.edition, 
      o.printer, o.publisher, 
      o.physical_description, o.accession_number, 
      o.old_accession_number, o.call_no, 
      o.credit_line, o.copyright_permission, 
      o.copyright, o.copyright_link_id, 
      o.da_link_id, o.technology_used, 
      o.department, t.title, o.update_date
    FROM object o, title t
    WHERE o.id = $id 
      AND t.object_id = o.id
      AND t.title_type = 'P'";

  my ($cnrID,$amicoID,$creatorFwd,$creatorRev,$createDt,
          $createStartYr,$createEndYr,$createLoc,$materialTechnique,$measure,
          $inscript,$workState,$edition,$printer,$publisher,$pysDesc, 
          $accessionNum,$oldAcessionNum,$callNum,$credits,$cPermission,
          $copyright,$cLinkID,$daLinkId,$technology,$dept, $title, $update_date) = $a->_get_one_row($query);
  return 0 if (!$cnrID); # this means there's no title!  WTF??

  my $xtitle = $dom->createElement("title");
  $xtitle->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($title))));
  $item->appendChild($xtitle);
  
  my $link = $dom->createElement("link");
  $link->appendChild($dom->createTextNode($a->link_id()));
  $item->appendChild($link);
  
  #Kambui Olujimi, (American, b. 1976). Something Like a Phenomenon, 2002. Digital photo collage 53 1/2 x 42 in. (135.9 x 106.7 cm). Brooklyn Museum, Gift of N.V. Hammer, by exchange, Dick S. Ramsay Fund and Charles Stewart Smith Memorial Fund. 2004.88
  
  my $desc = "";
  
  ################# creator stuff
  # sets first, collect into for displayable element later
  my $displayCreatorTxt = "";
  my $close_paren = 0;

  $query = "SELECT a.id, a.gender, a.nationality, a.ethnicity, a.date_text, " .
    "a.start_date, a.end_date, a.birth_place, a.death_place, a.active_date_place, " .
    "oa.role " .
    "FROM agent a, object_agent oa WHERE a.id = oa.agent_id and oa.object_id=$cnrID";
  my ($tmpref) = $a->_get_all_rows($query);
  foreach (@$tmpref) {
    my ($aid, $gender, $nationality, $ethnicity, $date_text, $start_date, $end_date, $birth_place,
        $death_place, $active_date_place, $role) = @$_;

    # get all names
    my $subq = "SELECT first_name, index_name, prefix, suffix, preferred_name " .
        "FROM names WHERE agent_id = $aid";
    my ($tmpref2) = $a->_get_all_rows($subq);
    
    my $gotname = 0;
    foreach (@$tmpref2) {
      my ($fname, $lname, $prefix, $suffix, $preferred) = @$_;
      # skip if no name??  why is this in our db??
      next if (!$fname and !$lname);
      $displayCreatorTxt .= ($displayCreatorTxt ? ", " : "") . $fname . ($fname ? " " : "") . $lname;
      $gotname = 1;
    }
    next if (!$gotname); # skip this whole creator

    # nationality
    if ($nationality) {
      $close_paren = 1;
      $displayCreatorTxt .= " ($nationality";
    }

    # vital dates
    if ($date_text) {
      if ($nationality) { $displayCreatorTxt .= ", $date_text"; }
      else { $displayCreatorTxt .= " ($date_text"; }
      $close_paren = 1;
    }
  } # creators
  $displayCreatorTxt .= ")" if ($close_paren);
  
  $desc .= "$displayCreatorTxt. $title";
  $desc .= ", $createDt" if ($createDt);
  $desc .= ". $pysDesc";
  $desc .= ", $measure" if ($measure);
  $desc .= ". ";
  $desc .= "Walker Art Center";
  $desc .= ", $credits" if ($credits);
  $desc .= ". $accessionNum.";
  
  
  ################## location
  my $loc = $a->location_name();
  if ($loc ne '0') {
    $desc = "On view at the Walker Art Center".($loc?", $loc":'')." <br/> <br/>".$desc;
  }
  
#Unicode::String->stringify_as('latin1');
#my $unicode_content = Unicode::String->new($desc);
#$desc = $unicode_content->utf8;
  
  my $description = $dom->createElement("description");
  $description->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($desc))));
  $item->appendChild($description);
  
  my $hasResources = 0;
  if ($cPermission) {
    # media first:
    $query = "SELECT m.id, r.file_name_path, r.file_name, m.description_term,
      m.caption, m.media_type, m.copyright
      FROM media m, renditions r, object_media om
      WHERE m.id = r.media_id
        AND m.id = om.media_id
        AND r.rendition_code = 'g'
        AND om.object_id = $id
        AND om.preferred_image = 't'
        LIMIT 1";
    ($tmpref) = $a->_get_all_rows($query);
    
    # wrapper element
    foreach (@$tmpref) {
      my ($mid, $path, $fn, $mterm, $mcaption, $mtype, $mcopyright) = @$_;
      $hasResources = 1;
      if (!$mediarss) {
        my $enclosure = $dom->createElement('enclosure');
        #http://www.walkerart.org/walker_images/e_images/01/wac_786e.jpg
        #$enclosure->appendChild($dom->createTextNode("http://www.walkerart.org/".$path."/".$fn));
        $enclosure->setAttribute('url',"http://www.walkerart.org/".$path."/".$fn);
        my $filesize = -s "/walker/axkit/www/".$path."/".$fn;
        $enclosure->setAttribute('length',$filesize);
        $enclosure->setAttribute('type',"image/jpeg");
        $item->appendChild($enclosure);
      } else {
        my $content = $dom->createElement('media:thumbnail');
        my $tpath = $path;
        my $tfn = $fn;
        #$tpath =~ s/\/images\//\/thumbs\//;
        #$tfn =~ s/g.jpg/j.jpg/;
        $content->setAttribute('url',"http://www.walkerart.org/".$tpath."/".$tfn);
        my $filesize = -s "/walker/axkit/www/".$tpath."/".$tfn;
        $content->setAttribute('length',$filesize);
        $content->setAttribute('type',"image/jpeg");
        $item->appendChild($content);
        $path =~ s/\/images\//\/e_images\//;
        $fn =~ s/g.jpg/e.jpg/;
        my $content = $dom->createElement('media:content');
        $content->setAttribute('url',"http://www.walkerart.org/".$path."/".$fn);
        my $filesize = -s "/walker/axkit/www/".$path."/".$fn;
        $content->setAttribute('length',$filesize);
        $content->setAttribute('type',"image/jpeg");
        $item->appendChild($content);
        my $tmp = $dom->createElement('media:title');
        $tmp->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($title))));
        $item->appendChild($tmp);
        $tmp = $dom->createElement('media:credit');
        $tmp->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($creatorFwd))));
        $item->appendChild($tmp);
        $tmp = $dom->createElement('media:copyright');
        $tmp->appendChild($dom->createTextNode(Table::Base::unwac(Table::Base::make_utf8($mcopyright))));
        $item->appendChild($tmp);
      }
    }
  }
  return 0 if (!$nomediaok && !$hasResources); # try to skip if no media...
  
  return $item;
}

sub oai_id {
  my ($self) = @_;
  return 'oai:walkerart.org/object/'.$$self{_art_id};
}

sub link_id {
  my ($self) = @_;
  return "http://collections.walkerart.org/item/object/".$$self{_art_id};
}

sub location_name {
  my ($self) = @_;
  my $query = "select l.description from object_location ol, location l
      where ol.object_id = ".$$self{_art_id}." and ol.location_id = l.id";
  my ($tmpref) = $self->_get_all_rows($query);
  foreach (@$tmpref) {
    my ($location) = @$_;
    return $location;
  }
  return 0;
}

1;
